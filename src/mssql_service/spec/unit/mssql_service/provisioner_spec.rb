require 'spec_helper'

describe VCAP::Services::Mssql::Provisioner do
  before do
    described_class.any_instance.stub(:initialize)
    subject.instance_variable_set(:@free_ports, {})
    subject.instance_variable_set(:@logger, getLogger)
  end

  describe ".generate_recipes" do
    context "for single node topology" do
      it "generates a valid recipes" do
        service_id = subject.generate_service_id
        version = "2012"
        plan = "free"
        best_nodes = [{
          "id" => "node1",
          "host" => "127.0.0.1"
        }]
        recipes = subject.generate_recipes(service_id, {plan.to_sym => {}}, version, best_nodes)
        config = recipes["configuration"]
        config.should be
        credentials = recipes["credentials"]
        credentials.should be

        config["peers"].should be
        config["version"].should eq(version)
        config["plan"].should eq(plan)
        config["peers"]["active"].should be
        config["peers"]["active"]["credentials"]["node_id"].should == "node1"

        config["backup_peer"].should == "node1"
        credentials["name"].should == service_id
        credentials["node_id"].should == "node1"
        credentials["port"].should == "1433"
      end
    end
  end

end
