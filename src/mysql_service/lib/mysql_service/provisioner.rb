# Copyright (c) 2009-2011 VMware, Inc.
require 'fileutils'
require 'redis'
require 'base64'
require 'securerandom'

$LOAD_PATH.unshift File.join(File.dirname __FILE__)
require 'common'
require 'job'
require 'utils'

class VCAP::Services::Mysql::Provisioner < VCAP::Services::Base::Provisioner
  include VCAP::Services::Mysql::Common
  include VCAP::Services::Mysql::Util
  attr_reader :free_ports
  attr_accessor :custom_resource_manager

  DEFAULT_PORTS_RANGE = (15000..16000)
  ACTIVE_ROLE = "active".freeze
  PASSIVE_ROLE = "passive".freeze

  def initialize(opts)
    super(opts)
    @free_ports = {}
    @custom_resource_manager = opts[:custom_resource_manager]
  end

  def create_backup_job
    VCAP::Services::Mysql::Backup::CreateBackupJob
  end

  def pre_send_announcement
    super
    %w[create_backup].each do |op|
      eval %[@node_nats.subscribe("#{service_name}.#{op}") { |msg, reply| on_#{op}(msg, reply) }]
    end
  end

  def varz_details
    varz = super

    @plan_mgmt.each do |plan, v|
      plan_nodes = @nodes.select { |_, node| node["plan"] == plan.to_s }.values
      if plan_nodes.size > 0
        available_capacity, max_capacity, used_capacity = compute_availability(plan_nodes)
        varz.fetch(:plans).each do |plan_detail|
          if (plan_detail.fetch(:plan) == plan)
            plan_detail.merge!({available_capacity: available_capacity})
            plan_detail.merge!({max_capacity: max_capacity})
            plan_detail.merge!({used_capacity: used_capacity})
          end
        end
      end
    end
    varz
  end

  # direct operate the hash is safe since gateway is single threaded
  def get_node_port(node_id)
    node_free_ports = @free_ports[node_id]
    unless node_free_ports
      initial_node_free_ports(node_id)
      node_free_ports = @free_ports[node_id]
    end
    raise "No ports available for #{node_id}" if node_free_ports.empty?
    free_port = node_free_ports.first
    node_free_ports.delete(free_port)
    free_port
  end

  def free_node_port(node_id, port)
    @free_ports[node_id] << port
  end

  def acquire_node_port(node_id, port)
    initial_node_free_ports(node_id) unless @free_ports[node_id]
    @free_ports[node_id].delete(port)
    port
  end

  def initial_node_free_ports(node_id)
    set = Set.new
    node_port_range.each {|p| set << p}
    @free_ports[node_id] = set
  end

  def node_port_range
    DEFAULT_PORTS_RANGE
  end

  ####
  # Generate mysql recipes for both single node(peer) and multiple peers topology.
  # recipes.configuration always contains information for all peers.
  #
  # recipes.credentials is customer facing connection string. It always contains
  # connection string for active peer due to backward compatiblity. However, credentials
  # also contains info of all peers in multi-peers topology, a multi-peers aware
  # application can connect to both active and passive peer on demand.
  #
  def generate_recipes(service_id, plan_config, version, best_nodes)
    credentials = {}
    configuration = {
      "version" => version,
      "plan" => plan_config.keys.first.to_s,
    }
    peers_config = []
    name = 'd' + generate_credential(password_length)
    user = 'u' + generate_credential(password_length)
    password = 'p' + generate_credential(password_length)

    # configure active node
    active_node = best_nodes.shift
    active_creds = gen_credential(
      active_node["id"], name, user, password, active_node["host"],
      get_node_port(active_node["id"]), service_id
    )
    active_peer_config = {
      "credentials" => active_creds,
      "role" => ACTIVE_ROLE
    }
    credentials = active_creds
    peers_config << active_peer_config

    # passive nodes
    best_nodes.each do |n|
      creds = gen_credential(
        n["id"], name, user, password, n["host"],
        get_node_port(n["id"]), service_id
      )
      passive_peer_config = {
        "credentials" => creds,
        "role" => PASSIVE_ROLE
      }
      peers_config << passive_peer_config
    end

    configuration["peers"] = peers_config
    credentials["peers"] = peers_config if best_nodes.size > 1
    configuration["backup_peer"] = get_backup_peer(credentials)

    recipes = VCAP::Services::Internal::ServiceRecipes.new
    recipes.credentials = credentials
    recipes.configuration = configuration
    recipes
  rescue => e
    @logger.error("Exception in generate_recipes, #{e}")
    @logger.error(e)
  end

  def gen_credential(node_id, database, username, password, host, port, service_id)
    {
      "node_id" => node_id,
      "name" => database,
      "hostname" => host,
      "host" => host,
      "port" => port,
      "user" => username,
      "username" => username,
      "password" => password,
      "uri" => generate_uri(username, password, host, port, database),
      "service_id" => service_id,
    }
  end

  # Setup various hooks to manitain free ports
  def after_delete_instance_handle(instance_handle)
    parse_node_ports(instance_handle) do |node_id, port|
      free_node_port(node_id, port)
    end
  end

  def after_add_instance_handle(instance_handle)
    parse_node_ports(instance_handle) do |node_id, port|
      acquire_node_port(node_id, port)
    end
  end

  def after_update_instance_handle(old_handle, new_handle)
    if old_handle
      parse_node_ports(old_handle) do |node_id, port|
        free_node_port(node_id, port)
      end
    end

    parse_node_ports(new_handle) do |node_id, port|
      acquire_node_port(node_id, port)
    end
  end

  def user_triggered_options(args)
    type = args["type"] || "full"
    {:type => type, :trigger_by => "user", :properties => args}
  end

  def periodically_triggered_options(args)
    type = args["type"] || "incremental"
    args.merge({:type => type, :trigger_by => "auto", :properties => args})
  end

  def on_create_backup(msg, reply)
    @logger.debug("Receive backup job response: #{msg}")
    backup_job_resp = BackupJobResponse.decode(msg)
    resp_to_worker = SimpleResponse.new

    if backup_job_resp.success
      @logger.info("Backup job #{backup_job_resp.properties} succeeded")
    else
      @logger.warn("Backup job #{backup_job_resp.properties} failed due to #{resp_to_controller.error}")
    end

    properties = backup_job_resp.properties
    f = Fiber.new do
      @custom_resource_manager.update_resource_properties(properties["update_url"], properties)
    end
    f.resume
    resp_to_worker.success = true
  rescue => e
    @logger.warn("Exception at on_create_backup: #{e}")
    @logger.warn(e)
    resp_to_worker.success = false
    resp_to_worker.error = e.to_s
  ensure
    @node_nats.publish(reply, resp_to_worker.encode)
  end

  private

  def parse_node_ports(handle)
    config = handle[:configuration]
    peers = config["peers"]
    peers.each do |peer|
      cred = peer["credentials"]
      node = cred["node_id"]
      port = cred["port"]
      raise "Failed to parse handle: #{handle}" unless (node && port)
      yield node, port
    end
  end

  def compute_availability(plan_nodes)
    max_capacity = plan_nodes.inject(0) { |sum, node| sum + node.fetch('max_capacity', 0) }
    available_capacity = plan_nodes.inject(0) { |sum, node| sum + node.fetch('available_capacity', 0) }
    used_capacity = max_capacity - available_capacity
    return available_capacity, max_capacity, used_capacity
  end

  def generate_uri(username, password, host, port, database)
    scheme = 'mysql'
    credentials = "#{username}:#{password}"
    path = "/#{database}"

    uri = URI::Generic.new(scheme, credentials, host, port, nil, path, nil, nil, nil)
    uri.to_s
  end

  def get_backup_peer(credentials)
    if credentials && credentials["peers"]
      passives = credentials["peers"].select {|p| p["role"] == PASSIVE_ROLE }
      passives[0]["credentials"]["node_id"] if passives.size > 0
    else
      credentials["node_id"]
    end
  end
end
