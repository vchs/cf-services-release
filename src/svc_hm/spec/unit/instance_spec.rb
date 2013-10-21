require 'spec_helper'

module ServicesHealthManager
  describe Instance do
    NODE_ID = 'NODE_1234'
    INSTANCE_ID= 'INST_4321'
    subject(:instance) { Instance.new(INSTANCE_ID, {}) }

    it "should process heartbeat message correctly" do
      node_info = { node_id: NODE_ID, node_type: 'svc1', node_ip: '1.2.3.4'}
      state =  { health: 'ok'}
      msg, _ = instance.process_heartbeat(node_info, state)
      msg.start_with?('svc1.health.ok').should be_true
      state =  { health: 'fail'}
      msg, _ = instance.process_heartbeat(node_info, state)
      msg.start_with?('svc1.health.alert').should be_true
    end
  end
end
