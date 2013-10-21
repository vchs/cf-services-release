require 'spec_helper'

module ServicesHealthManager
  describe Peer do
    let(:peer) { Peer.new(timeout: 2) }

    it "should process heartbeat message" do
      peer.alive?.should_not be_true
      peer.has_recent_heartbeat?.should_not be_true
      peer.receive_heartbeat('ok').should == 1
      peer.alive?.should be_true
      peer.has_recent_heartbeat?.should be_true
      peer.lose_heartbeat
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
end
