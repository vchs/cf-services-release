# Copyright (c) 2013-2015 VMware, Inc.

require_relative './common'
require_relative './util'
require_relative './message_queue'
require_relative './task'

class VCAP::Services::MSSQL::Provisioner < VCAP::Services::Base::Provisioner
  include VCAP::Services::MSSQL::Messages
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
    %w[backup].each do |op|
      eval %[@node_nats.subscribe("#{service_name}.#{op}") { |msg, reply| on_#{op}(msg, reply) }]
    end
  end

  # Called by MSSQL ResourceManager which handle the http request coms from SC
  def create_backup(service_id, backup_id, opts = {}, &blk)
    @logger.debug("Backup task for service_id=#{service_id}")
    @logger.debug("Backup task opts: #{opts}")

    options = {
      :id         => generate_credential,
      :name       => "backup",
      :node_id    => find_backup_peer(service_id), # used to identify queue "#{queue_name}:q:#{node_id}"
      :service_id => service_id,
      :backup_id  => backup_id,
      :properties => opts # user_triggered_options({:service_id => xxx, :backup_id => xxx, :update_url => xxx, ...})
    }

    BackupTask.create(options)

    @logger.info("BackupTask created: #{options}")
    blk.call(success)
  rescue => e
    @logger.warn("BackupTask failed: #{e}")
    @logger.warn(e)
    blk.call(failure(e))
  end

  # Called by base provisioner(provisioner_v3.rb), and we should overried it here
  def restore_backup(service_id, backup_id, node_id, ori_service_id, opts)
   @logger.debug("Restore task for service_id=#{service_id}")
   @logger.debug("Restore task opts: #{opts}")

   # as for MSSQL restoring and it needs the credential from VCAP::Services::Internal::ServiceRecipes
   recipes = opts[:recipes]
   peer = recipes.configuration["peers"].find { |peer| peer["credentials"]["node_id"] == node_id }
   raise ServiceError.new(ServiceError::NOT_FOUND, "Can't find peer for #{node_id} ") if peer.nil?

   options = {
     :id                     => generate_credential,
     :name                   => "restore",
     :node_id                => node_id, # used to identify queue "#{queue_name}:q:#{node_id}"
     :service_id             => service_id,
     :original_database_name => find_credential_name(ori_service_id),
     :original_service_id    => ori_service_id,
     :backup_id              => backup_id,
     :credential             => peer["credentials"]
   }

   # Handle restore response from Node through NATS(MSSQL.restore_backup.service_id) in the provisioner_v3.rb
   RestoreTask.create(options)

   @logger.info("RestoreTask created: #{options}")
   true
  rescue => e
   @logger.warn("RestoreTask failed: #{e}")
   @logger.warn(e)
   false
  end

  # Handle backup response from Node throug NATS(MSSQL.backup)
  def on_backup(msg, reply)
    @logger.debug("Receive backup task response: #{msg}")
    rep = BackupTaskResponse.decode(msg)
    simple_rep = SimpleResponse.new

    if rep.result.upcase.eql? "OK"
      @logger.info("Backup task #{rep.properties} succeeded")
    else
      @logger.warn("Backup task #{rep.properties} failed due to #{rep.result}")
    end

    properties = rep.properties
    f = Fiber.new do
      @custom_resource_manager.update_resource_properties(properties["update_url"], properties)
    end
    f.resume
    simple_rep.success = true
  rescue => e
    @logger.warn("Exception at on_backup: #{e}")
    @logger.warn(e)
    simple_rep.success = false
    simple_rep.error = e.to_s
  ensure
    @node_nats.publish(reply, simple_rep.encode)
  end

  # Generate properties for Restoring and Provisioning
  def gen_provision_properties(service_id)
    status = is_restoring?(service_id) ? "Restoring" : "Provisioning"
    { :status => status }
  end

  def user_triggered_options(args)
    args.merge({ :type => args[:type] || "full", :trigger_by => "user" })
  end

  def periodically_triggered_options(args)
  end

  def find_credential_name(service_id)
    svc = get_instance_handle(service_id)
    raise ServiceError.new(ServiceError::NOT_FOUND, "service id #{service_id}") if svc.nil?
    name = svc[:credentials]["name"]
    raise "Cannot find name for #{service_id}" if name.nil?
    name
  end

  # Generate MSSQL recipes for both single node(peer) and multiple peers topology.
  def generate_recipes(service_id, plan_config, version, best_nodes, original_credential=nil)
    credentials = {}
    configuration = {
      "version" => version,
      "plan" => plan_config.keys.first.to_s,
    }
    peers_config = []
    # Must not prefix with number
    name = "d" + generate_credential(dbname_length)
    user = "u" + generate_credential(dbname_length)
    password = "p" + generate_credential(password_length)

    # If it's restoring, and we still generate different database name on the fly
    if original_credential && original_credential.has_key?("user") && original_credential.has_key?("password")
      user, password = %w(user password).map { |key| original_credential[key] }
    end

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
    configuration["backup_peer"] = get_backup_peer credentials
    configuration["properties"] = gen_provision_properties service_id

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
        available_capacity, max_capacity, used_capacity = compute_availability plan_nodes
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
BackupTask = VCAP::Services::MSSQL::BackupTask
RestoreTask = VCAP::Services::MSSQL::RestoreTask
