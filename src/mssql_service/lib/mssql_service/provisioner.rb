# Copyright (c) 2009-2011 VMware, Inc.
require 'securerandom'
require 'uri'

$LOAD_PATH.unshift File.join(File.dirname __FILE__)
require 'common'

class VCAP::Services::Mssql::Provisioner < VCAP::Services::Base::Provisioner
  include VCAP::Services::Mssql::Common

  def initialize(opts)
    super(opts)
  end

  #FIXME: a stub function
  def provision_service(request, prov_handle=nil, &blk)
    super(request, prov_handle, &blk)

    #NOTE: Since we do not have HM, and we do not want the timer alarm,
    #      so, we hack it.
    @instance_provision_callbacks.each { |instance_id, callbacks|
      @logger.debug("fire success callback for #{instance_id}")
      callbacks = @instance_provision_callbacks[instance_id]
      timer = callbacks[:timer]
      EM.cancel_timer(timer)
      callbacks[:success].call
      @instance_provision_callbacks.delete(instance_id)
    }
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
    active_node_credential = gen_credential(
      active_node["id"],
      name,
      user,
      password,
      active_node["host"],
      active_node["port"] || get_default_port
    )
    credentials = active_node_credential
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
      passive_node_credential = gen_credential(
        n["id"],
        name,
        user,
        password,
        n["host"],
        active_node["port"] || get_default_port
      )

      configurations["peers"]["passive"] ||= []
      configurations["peers"]["passive"] << { "credentials" => passive_node_credential }
    end

    configurations["backup_peer"] = get_backup_peer(credentials)

    recipes = {
      "credentials" => credentials,
      "configuration" => configurations,
    }
  rescue => e
    @logger.error "Exception in generate_recipes, #{e}"
  end

  def generate_service_id
    flavor + SecureRandom.uuid.to_s.gsub(/-/, '')
  end

  def varz_details
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

private

  def compute_availability(plan_nodes)
    max_capacity = plan_nodes.inject(0) { |sum, node| sum + node.fetch('max_capacity', 0) }
    available_capacity = plan_nodes.inject(0) { |sum, node| sum + node.fetch('available_capacity', 0) }
    used_capacity = max_capacity - available_capacity
    return available_capacity, max_capacity, used_capacity
  end

  VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a
  def generate_credential(length = 12)
    Array.new(length) { VALID_CREDENTIAL_CHARACTERS[rand(VALID_CREDENTIAL_CHARACTERS.length)] }.join
  end

  def get_default_port
    "1433"
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

  def get_backup_peer(credentials)
    if credentials && credentials["peers"] && passives = credentials["peers"]["passive"]
      passives[0]["node_id"] if passives.size > 0
    else
      credentials["node_id"]
    end
  end

end
