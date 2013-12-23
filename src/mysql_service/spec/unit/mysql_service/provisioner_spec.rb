require 'spec_helper'

describe VCAP::Services::Mysql::Provisioner do
  before do
    described_class.any_instance.stub(:initialize)
    subject.instance_variable_set(:@free_ports, {})
    subject.instance_variable_set(:@logger, getLogger)
    described_class.any_instance.stub(:is_restoring?).and_return(false)
  end

  describe ".generate_recipes" do
    shared_context 'generate correct credentials' do
      it 'be able to reuse password from original_creds' do
        test_str = "n" * subject.dbname_length
        creds = {
          "password" => "p#{test_str}"
        }

        recipes = subject.generate_recipes(@service_id, {@plan.to_sym => {}}, @version, @best_nodes,
                                           { 'original_credentials' => creds })
        credentials = recipes.credentials
        credentials['password'].should eq creds['password'] }
        peers = recipes.configuration["peers"]
        peers.each do |peer|
          peer_creds = peer["credentials"]
          peer_creds['password'].should eq creds['password'] }
        end
      end

      it 'should support password overwritten' do
        test_str = "n" * subject.dbname_length
        creds = {
            "name"     => "d#{test_str}",
            "user"     => "u#{test_str}",
            "password" => "p#{test_str}"
        }

        NEW_PASSWORD = 'newpassword'
        recipes = subject.generate_recipes(@service_id, {@plan.to_sym => {}}, @version, @best_nodes,
                                           { 'original_credentials' => creds,
                                             'user_specified_credentials' => { 'password' => NEW_PASSWORD }
                                           })
        credentials = recipes.credentials
        %w(name user).each { |key| credentials[key].should eq creds[key] }
        credentials['password'].should eq NEW_PASSWORD
        peers = recipes.configuration["peers"]
        peers.each do |peer|
          peer_creds = peer["credentials"]
          peer_creds['password'].should eq NEW_PASSWORD
        end
      end

      it 'raise error if password isn\'t given' do
        expect {
          subject.generate_recipes(@service_id, {@plan.to_sym => {}},
                                   @version, @best_nodes, {}, {})
        }.to raise_error
      end
    end

    before do
      @service_id = subject.generate_service_id
      @version = "5.6"
      @plan = "free"
      @best_nodes = [{
        "id" => "node1",
        "host" => "192.168.1.1"
      }]

    end

    context "for single peer topology" do
      include_context "generate correct credentials"

      it "generates a valid recipes" do
        recipes = subject.generate_recipes(@service_id, {@plan.to_sym => {}}, @version, @best_nodes,
                                           { 'user_specified_credentials' => { 'password' => 'password' }
                                           })
        config = recipes.configuration
        config.should be
        credentials = recipes.credentials
        credentials.should be

        config["peers"].should be_instance_of(Array)
        config["version"].should eq(@version)
        config["plan"].should eq(@plan)
        active_peer = config["peers"].find{|p| p["role"] == "active"}
        active_peer.should be
        active_peer["credentials"]["node_id"].should == "node1"

        config["backup_peer"].should == "node1"
        credentials["service_id"].should eq @service_id
        credentials["name"].should_not == @service_id
        credentials["node_id"].should == "node1"
        credentials["port"].should == VCAP::Services::Mysql::Provisioner::DEFAULT_PORTS_RANGE.first
      end

      it "limit the dbname & user name length" do
        recipes = subject.generate_recipes(@service_id, {@plan.to_sym => {}}, @version, @best_nodes,
                                           { 'user_specified_credentials' => { 'password' => 'password' }
                                           })

        name, username, passwd = %w{name username password}.map {|k| recipes.credentials[k]}
        name.length.should eq(subject.dbname_length + 1) # with one prefix 'd'
        username.length.should == (subject.password_length + 1)
      end
    end

    context "for multiple peers topology" do
      include_context "generate correct credentials"

      before do
        @service_id = subject.generate_service_id
        @version = "5.6"
        @plan = "free"
        @best_nodes = [{
          "id" => "node1",
          "host" => "192.168.1.1"
        }, {
          "id" => "node2",
          "host" => "192.168.1.2"
        }, {
          "id" => "node3",
          "host" => "192.168.1.3"
        }]
      end

      it "generate valid recipes" do
        recipes = subject.generate_recipes(@service_id, {@plan.to_sym => {}}, @version, @best_nodes,
                                           { 'user_specified_credentials' => { 'password' => 'password' }
                                           })
        peers = recipes.configuration["peers"]
        active_peers = peers.select {|p| p["role"] == "active"}
        passive_peers = peers.select {|p| p["role"] == "passive"}
        active_peers.size.should eq(1)
        passive_peers.size.should eq(2)

        credentials = recipes.credentials
        credentials["peers"].size.should eq(3)
      end

      it "use passive peer as backup peer" do
        recipes = subject.generate_recipes(@service_id, {@plan.to_sym => {}}, @version, @best_nodes,
                                           { 'user_specified_credentials' => { 'password' => 'password' }
                                           })
        peers = recipes.configuration["peers"]
        backup_peer_id = recipes.configuration["backup_peer"]
        backup_peer = peers.find {|p| p["credentials"]["node_id"] == backup_peer_id }
        backup_peer["role"].should eq("passive")
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
            "peers" => [
              {
                "role" => "active",
                "credentials" => {
                  "node_id" => "node1",
                  "port" => 10001
                }
              }
            ]
          }
        }
        subject.after_add_instance_handle(instance_handle)
        expect{ subject.get_node_port("node1")}.to raise_error(/No ports/)

        subject.after_delete_instance_handle(instance_handle)
        expect{ subject.get_node_port("node1")}.to_not raise_error
      end
    end

    context "handle new node" do
      it "able to fetch free port" do
        subject.get_node_port("node2").should == 10000
      end
    end

    describe ".after_update_instance_handle" do
      before do
        @node_id = "node1"
        @node_port = 10001
        @new_node_id= "node2"
        @new_node_port = 10000

        @old_handle = {
          :configuration => {
            "peers" => [
              {
                "role" => "active",
                "credentials" => {
                  "node_id" => @node_id,
                  "port" => @node_port
                }
              }
            ]
          }
        }

        @new_handle = {
          :configuration => {
            "peers" => [
              {
                "role" => "passive",
                "credentials" => {
                  "node_id" => @new_node_id,
                  "port" => @new_node_port
                }
              }
            ]
          }
        }

        subject.initial_node_free_ports(@node_id)
      end

      it "should update free port used by handles" do
        subject.after_update_instance_handle(@old_handle, @new_handle)
        subject.free_ports[@node_id].include?(@node_port).should be
        subject.free_ports[@new_node_id].include?(@new_node_port).should be_false
      end
    end

    describe ".acquire_node_port" do
      it "acquires port for given node" do
        node_id = "node1"
        subject.acquire_node_port(node_id, 10000)
        subject.free_ports[node_id].include?(10000).should be_false
      end
    end
  end
end
