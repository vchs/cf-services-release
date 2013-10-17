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

    it "should process heartbeat message" do
      health = {test: 123}
      manager.should_receive(:process_heartbeat).with(health)
      message_bus.publish(Common::CHAN_HEARTBEAT, health)
    end
  end
end
