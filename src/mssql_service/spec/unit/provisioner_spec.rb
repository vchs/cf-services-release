require_relative "../spec_helper"

describe VCAP::Services::MSSQL::Provisioner do

  before do
    described_class.any_instance.stub(:initialize)
    subject.instance_variable_set(:@logger, getLogger)
  end

  describe "#generate_recipes" do
    context "when single node topology" do
      it "should generates a valid recipe" do
        service_id = subject.generate_service_id
        node_id = "mssql_node_free_1"
        version = "MSSQLSERVER2008R2"
        host = "192.168.56.100"
        best_nodes = [{
          "id" => node_id,
          "host" => host,
          "port" => 9999
        }]
        plan_config = {:free => {:lowwater => 10}}

        recipes = subject.generate_recipes(service_id, plan_config , version, best_nodes)

        config = recipes.configuration
        config.should be_instance_of Hash
        config["version"].should eq version
        config["plan"].should eq "free"

        credentials = recipes.credentials
        credentials.should be_instance_of Hash
        credentials["peers"].should be_nil

        peers = config["peers"]
        peers.should be_instance_of Array

        credentials.should be peers[0]["credentials"]
        credentials["service_id"].should eq service_id
        credentials["node_id"].should eq node_id
        credentials["port"].should eq 9999
      end
    end
  end

  describe "#get_port" do
    it "should be get default port for MSSQL" do
      subject.get_port(nil).should eq 1433
    end

    it "should be get port for MSSQL" do
      subject.get_port(9999).should eq 9999
    end
  end
end
