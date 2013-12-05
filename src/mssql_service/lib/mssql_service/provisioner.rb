# Copyright (c) 2013-2015 VMware, Inc.

require_relative './common'
require_relative './util'
require_relative './message_queue'
require_relative './job'

class VCAP::Services::MSSQL::Provisioner < VCAP::Services::Base::Provisioner
  include VCAP::Services::MSSQL::Common
  include VCAP::Services::MSSQL::Util

  attr_accessor :custom_resource_manager

  ACTIVE_ROLE = "active".freeze
  PASSIVE_ROLE = "passive".freeze

  def initialize(opts)
    super(opts)
    @custom_resource_manager = opts[:custom_resource_manager]
  end

  def pre_send_announcement
    super
    addition_opts = self.options[:additional_options]
    if addition_opts && addition_opts[:redis]
      MessageQueue.redis = addition_opts[:redis]
    end
    %w[create_backup].each do |op|
      eval %[@node_nats.subscribe("#{service_name}.#{op}") { |msg, reply| on_#{op}(msg, reply) }]
    end
  end

  # Called by MSSQL ResourceManager which handle the http request coms from SC
  def create_backup(service_id, backup_id, opts = {}, &blk)
    @logger.debug("Create backup job for service_id=#{service_id}")

    @logger.debug(opts)

    options = {
                :id         => generate_credential,
                :name       => "backup",
                :service_id => service_id,
                :backup_id  => backup_id,
                :node_id    => find_backup_peer(service_id),
                :metadata   => backup_metadata(service_id).merge(opts)
              }

    BackupJob.create(options)

    @logger.info("CreateBackupJob created: #{options}")
    blk.call(success)
  rescue => e
    @logger.warn("CreateBackupJob failed: #{e}")
    @logger.warn(e)
    blk.call(failure(e))
  end

  # Receive backup response from Node throug NATS: MSSQL.create_backup
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

  def user_triggered_options(args)
    type = args["type"] || "full"
    {:type => type, :trigger_by => "user", :properties => args}
  end

  def periodically_triggered_options(args)
    type = args["type"] || "differentiate"
    args.merge({:type => type, :trigger_by => "auto", :properties => args})
  end

  def generate_recipes(service_id, plan_config, version, best_nodes)
    credentials = {}
    configuration = {
      "version" => version,
      "plan" => plan_config.keys.first.to_s,
    }
    peers_config = []
    # Must not prefix with number
    name = 'd' + generate_credential(dbname_length)
    user = 'u' + generate_credential(dbname_length)
    password = 'p' + generate_credential(password_length)

    # configure active node
    active_node = best_nodes.shift
    active_node_credential = gen_credential(
      service_id,
      active_node["id"],
      name,
      user,
      password,
      active_node["host"],
      get_port(active_node["port"])
    )

    active_peer_config = {
      "credentials" => active_node_credential,
      "role" => ACTIVE_ROLE
    }

    credentials = active_node_credential
    peers_config << active_peer_config

    # passive nodes
    best_nodes.each do |n|
      passive_node_credential = gen_credential(
        service_id,
        n["id"],
        name,
        user,
        password,
        n["host"],
        get_port(n["port"])
      )

      passive_peer_config = {
        "credentials" => passive_node_credential,
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
    @logger.error "Exception in generate_recipes, #{e}"
  end

  def varz_details
    #varz = {
    #  :nodes => @nodes,
    #  :prov_svcs => svcs,
    #  :orphan_instances => orphan_instances,
    #  :orphan_bindings => orphan_bindings,
    #  :plans => plan_mgmt,
    #  :responses_metrics => @responses_metrics,
    #}
    varz = super

    @plan_mgmt.each do |plan, v|
      plan_nodes = @nodes.select { |_, node| node["plan"] == plan.to_s }.values

      if plan_nodes.size > 0
        available_capacity, max_capacity, used_capacity = compute_availability(plan_nodes)
        varz.fetch(:plans).each do |plan_detail|
          if (plan_detail.fetch(:plan) == plan)
            plan_detail.merge!({
              :available_capacity => available_capacity,
              :max_capacity => max_capacity,
              :used_capacity => used_capacity
            })
          end
        end
      end
    end

    varz
  end

  def get_port(port)
    port || 1433
  end

  private

  def get_backup_peer(credentials)
    if credentials && credentials["peers"]
      passives = credentials["peers"].select {|p| p["role"] == PASSIVE_ROLE }
      passives[0]["credentials"]["node_id"] if passives.size > 0
    else
      credentials["node_id"]
    end
  end

  def compute_availability(plan_nodes)
    max_capacity = plan_nodes.inject(0) { |sum, node| sum + node.fetch('max_capacity', 0) }
    available_capacity = plan_nodes.inject(0) { |sum, node| sum + node.fetch('available_capacity', 0) }
    used_capacity = max_capacity - available_capacity

    return available_capacity, max_capacity, used_capacity
  end

  def gen_credential(service_id, node_id, database, username, password, host, port)
    {
      "service_id" => service_id,
      "node_id" => node_id,
      "name" => database,
      "hostname" => host,
      "host" => host,
      "port" => port,
      "user" => username,
      "username" => username,
      "password" => password,
      "uri" => generate_uri(username, password, host, port, database)
    }
  end

  def generate_uri(username, password, host, port, database)
    scheme = 'mssql'
    credentials = "#{username}:#{password}"
    path = "/#{database}"

    uri = URI::Generic.new(scheme, credentials, host, port, nil, path, nil, nil, nil)
    uri.to_s
  end
end

# Alias
MessageQueue = VCAP::Services::MSSQL::MessageQueue
BackupJob = VCAP::Services::MSSQL::BackupJob