require 'spec_helper'

module ServicesHealthManager
  describe Peer do
    subject(:peer) { Peer.new(timeout: 2) }

    it "should process heartbeat message" do
      peer.alive?.should_not be_true
      peer.has_recent_heartbeat?.should_not be_true
      peer.receive_heartbeat('ok').should == 1
      peer.alive?.should be_true
      peer.has_recent_heartbeat?.should be_true
      peer.down!
      peer.alive?.should_not be_true
      peer.receive_heartbeat('ok')
      sleep(1)
      peer.alive?.should be_true
      sleep(2)
      peer.alive?.should_not be_true
      peer.has_recent_heartbeat?.should_not be_true
      peer.receive_heartbeat('fail') == -1
      peer.alive?.should_not be_true
      peer.has_recent_heartbeat?.should be_true
    end
  end



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
      msg.start_with?('svc1.health.remedy').should be_true
    end
  end



end
