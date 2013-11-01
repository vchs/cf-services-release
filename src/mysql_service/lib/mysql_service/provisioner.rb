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

  DEFAULT_PORTS_RANGE = (15000..16000)
  def initialize(opts)
    super(opts)
    @free_ports = {}
  end

  def create_snapshot_job
    VCAP::Services::Mysql::Snapshot::CreateSnapshotJob
  end

  def rollback_snapshot_job
    VCAP::Services::Mysql::Snapshot::RollbackSnapshotJob
  end

  def delete_snapshot_job
    VCAP::Services::Base::AsyncJob::Snapshot::BaseDeleteSnapshotJob
  end

  def create_serialized_url_job
    VCAP::Services::Base::AsyncJob::Serialization::BaseCreateSerializedURLJob
  end

  def import_from_url_job
    VCAP::Services::Mysql::Serialization::ImportFromURLJob
  end

  def create_backup_job
    VCAP::Services::Mysql::Backup::CreateBackupJob
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

  def generate_service_id
    'd' + SecureRandom.uuid.to_s.gsub(/-/, '')
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

  def initial_node_free_ports(node_id)
    set = Set.new
    node_port_range.each {|p| set << p}
    @free_ports[node_id] = set
  end

  def node_port_range
    DEFAULT_PORTS_RANGE
  end

  def generate_recipes(service_id, plan_config, version, best_nodes)
    recipes = {}
    credentials = {}
    configurations = {}
    name = service_id
    user = 'u' + generate_credential
    password = 'p' + generate_credential

    # configure active node
    active_node = best_nodes.shift
    active_creds = gen_credential(
      active_node["id"], name, user, password, active_node["host"],
      get_node_port(active_node["id"])
    )
    credentials = active_creds
    configurations = {
      "version" => version,
      "plan" => plan_config.keys[0].to_s,
      "peers" => {
        "active" => {
          "credentials" => credentials
        }
      }
    }

    # passive nodes
    best_nodes.each do |n|
      creds = gen_credential(
        n["id"], name, user, password, n["host"],
        get_node_port(n["id"])
      )
      credentials["peers"] ||= {}
      credentials["peers"]["passive"] ||= []
      credentials["peers"]["passive"] << creds
    end

    configurations["backup_peer"] = get_backup_peer(credentials)

    recipes = {
      "credentials" => credentials,
      "configuration" => configurations,
    }
    return recipes
  rescue => e
    @logger.error("Exception in generate_recipes, #{e}")
    @logger.error(e)
  end

  def gen_credential(node_id, database, username, password, host, port)
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
      @free_ports[node_id].delete(port)
    end
  end

  def after_update_instance_handle(old_handle, new_handle)
    if old_handle
      parse_node_ports(old_handle) do |node_id, port|
        free_node_port(node_id, port)
      end
    end

    parse_node_ports(new_handle) do |node_id, port|
      @free_ports[node_id].delete(port)
    end
  end

  def user_triggered_options(params)
    type = params["type"] || "full"
    {:type => type}
  end

  def periodically_triggered_options(params)
    type = params["type"] || "incremental"
    {:type => type}
  end

  private

  def parse_node_ports(handle)
    config = handle[:configuration]
    peers = config["peers"]
    peers.each do |role, peer|
      cred = peer["credentials"]
      node = cred["node_id"]
      port = cred["port"]
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
    if credentials && credentials["peers"] && passives = credentials["peers"]["passive"]
      passives[0]["node_id"] if passives.size > 0
    else
      credentials["node_id"]
    end
  end
end
