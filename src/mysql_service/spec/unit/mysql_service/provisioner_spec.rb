require 'spec_helper'

describe VCAP::Services::Mysql::Provisioner do
  before do
    described_class.any_instance.stub(:initialize)
    subject.instance_variable_set(:@free_ports, {})
    subject.instance_variable_set(:@logger, getLogger)
  end

  describe ".generate_recipes" do
    context "for single node topology" do
      it "generates a valid recipes" do
        service_id = subject.generate_service_id
        version = "5.6"
        plan = "free"
        best_nodes = [{
          "id" => "node1",
          "host" => "192.168.1.1"
        }]
        recipes = subject.generate_recipes(service_id, {}, plan, version, best_nodes)
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
        credentials["port"].should == VCAP::Services::Mysql::Provisioner::DEFAULT_PORTS_RANGE.first
      end
    end
  end

  describe ".get_node_port" do
    before do
      subject.stub(:node_port_range) {(10000..10001)}
    end

    context "handle existing nodes" do
      it "able to fetch free port" do
        subject.get_node_port("node1").should == 10000
      end

      it "able to maintain free ports status" do
        subject.initial_node_free_ports("node1")
        subject.get_node_port("node1").should == 10000
        instance_handle = {
          :configuration => {
            "peers" => {
              "active" => {
                "node_id" => "node1",
                "port" => 10001
              }
            }
          }
        }
        subject.after_add_instance_handle(instance_handle)
        expect{ subject.get_node_port("node1")}.to raise_error /No ports/

        subject.after_delete_instance_handle(instance_handle)
        expect{ subject.get_node_port("node1")}.to_not raise_error
      end
    end

    context "handle new node" do
      it "able to fetch free port" do
        subject.get_node_port("node2").should == 10000
      end
    end
  end
end
