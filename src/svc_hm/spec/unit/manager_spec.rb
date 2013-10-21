require 'spec_helper'

module ServicesHealthManager
  describe Manager do
    let(:message_bus) { CfMessageBus::MockMessageBus.new }
    let(:manager) do
      m = Manager.new(get_local_config)
      m.instance_variable_set("@message_bus", message_bus)
      m.setup_nats_route
      m
    end

    it "should listen to heartbeat channel" do
      health = {test: 123}
      manager.should_receive(:process_heartbeat).with(health)
      message_bus.publish(Common::CHAN_HEARTBEAT, health)
    end

    it "should process heartbeat successfully" do
      NODE_ID = '123'
      message = { node_id: NODE_ID, node_type: 'mysql', node_ip: '1.2.3.4', instances: { '1' => { health: 'ok'} } }
      manager.process_heartbeat(message)
      actual = manager.get_instance('1').actual_topology
      actual[NODE_ID].alive?.should be_true
    end
  end
end
