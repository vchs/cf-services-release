require "spec_helper"

describe "MSSQL Gateway", type: :integration, component: [:nats, :ccng] do
  before do
    login_to_ccng_as('12345', 'sre@vmware.com')
  end

  it 'start nats and ccng as required' do
    component!(:nats).should be
    ccng_get('/v2/services').should be
  end

  it 'can provision a new service instance' do
    ccng_get("/v2/spaces/#{space_guid}/service_instances").fetch("total_results").should == 0
    provision_mssql_instance("111111")
    provision_mssql_instance("222222")
  end

  def space_guid
    component!(:ccng).space_guid
  end

  def provision_mssql_instance(name)
    inst_data = ccng_post "/v2/service_instances",
      name: name,
      space_guid: space_guid,
      service_plan_guid: plan_guid("mssql", "free")
    inst_data.fetch("metadata").fetch("guid")
  end

  def plan_guid(service_name, plan_name)
    plans_path = service_response(service_name).fetch("entity").fetch("service_plans_url")
    plan_response(plan_name, plans_path).fetch('metadata').fetch('guid')
  end

private

  def plan_response(plan_name, plans_path)
    with_retries(30) do
      response = client.get "http://localhost:8181/#{plans_path}", header: { "AUTHORIZATION" => ccng_auth_token }
      res = Yajl::Parser.parse(response.body)
      res.fetch("resources").detect {|p| p.fetch('entity').fetch('name') == plan_name } or
        raise "Could not find plan with name #{plan_name.inspect} in response #{response.body}"
    end
  end

  def service_response(service_name)
    with_retries(30) do
      response = client.get "http://localhost:8181/v2/services", header: { "AUTHORIZATION" => ccng_auth_token }

      res = Yajl::Parser.parse(response.body)
      res.fetch("resources").detect {|service| service.fetch('entity').fetch('label') == service_name } or
        raise "Could not find a service with name #{service_name} in #{response.body}"
    end
  end
end
