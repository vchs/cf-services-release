# Copyright (c) 2013-2015 VMware, Inc.
require 'securerandom'

require_relative "./common"

class VCAP::Services::MSSQL::Provisioner < VCAP::Services::Base::Provisioner
  include VCAP::Services::MSSQL::Common

  PASSWORD_LENGTH = 9
  ACTIVE_ROLE = "active".freeze
  PASSIVE_ROLE = "passive".freeze

  def initialize(opts)
    super(opts)
  end

  def generate_recipes(service_id, plan_config, version, best_nodes)
    credentials = {}
    configuration = {
      "version" => version,
      "plan" => plan_config.keys.first.to_s,
    }
    peers_config = []
    name = service_id
    # Must not prefix with number
    user = "u" + generate_credential(password_length)
    password = "p" + generate_credential(password_length)

    # configure active node
    active_node = best_nodes.shift
    active_node_credential = gen_credential(
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

    recipes = VCAP::Services::Internal::ServiceRecipes.new
    recipes.credentials = credentials
    recipes.configuration = configuration
    recipes
  rescue => e
    @logger.error "Exception in generate_recipes, #{e}"
  end

  def generate_service_id
    "s" + SecureRandom.uuid.to_s.gsub(/-/, '')[0, 9]
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

  def compute_availability(plan_nodes)
    max_capacity = plan_nodes.inject(0) { |sum, node| sum + node.fetch('max_capacity', 0) }
    available_capacity = plan_nodes.inject(0) { |sum, node| sum + node.fetch('available_capacity', 0) }
    used_capacity = max_capacity - available_capacity

    return available_capacity, max_capacity, used_capacity
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

  def generate_credential(length=9)
    SecureRandom.uuid.to_s.gsub(/-/, '')[0, length]
  end

  def password_length
    PASSWORD_LENGTH
  end
end
