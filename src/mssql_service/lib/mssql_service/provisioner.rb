# Copyright (c) 2013-2015 VMware, Inc.
require 'securerandom'

require_relative "./common"

class VCAP::Services::MSSQL::Provisioner < VCAP::Services::Base::Provisioner
  include VCAP::Services::MSSQL::Common

  def initialize(opts)
    super(opts)
  end

  def generate_recipes(service_id, plan_config, version, best_nodes)
    @logger.debug "plan_config: #{plan_config}"
    recipes = {}
    credentials = {}
    configurations = {}
    name = service_id
    # Must not prefix with number
    user = "u" + SecureRandom.uuid.to_s.gsub(/-/, '')
    password = "p" + SecureRandom.uuid.to_s.gsub(/-/, '')

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
        get_port(n["port"])
      )

      credentials["peers"] ||= {}
      credentials["peers"]["passive"] ||= []
      credentials["peers"]["passive"] << passive_node_credential
    end

    recipes = {
      "credentials" => credentials,
      "configuration" => configurations,
    }

    return recipes
  rescue => e
    @logger.error "Exception in generate_recipes, #{e}"
  end

  def generate_service_id
    flavor + SecureRandom.uuid.to_s.gsub(/-/, '')
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
end
