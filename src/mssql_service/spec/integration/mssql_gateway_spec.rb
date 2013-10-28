require "spec_helper"
require "sequel"

describe "MSSQL Gateway", type: :integration, components: [:nats, :ccng, :mssql] do

  before do
    login_to_ccng_as('12345', 'sre@vmware.com')
  end

  it 'can provision a new service instance' do
    ccng_get("/v2/spaces/#{space_guid}/service_instances").fetch("total_results").should == 0
    provision_mssql_instance("111111")
    provision_mssql_instance("222222")
  end

  def provision_mssql_instance(name)
    inst_data = ccng_post "/v2/service_instances",
      name: name,
      space_guid: space_guid,
      service_plan_guid: plan_guid("mssql", "free")
    inst_data.fetch("metadata").fetch("guid")
  end

end
