require "spec_helper"

module BackupManager
  describe Manager do
    let(:message_bus) { CfMessageBus::MockMessageBus.new }
    let(:manager) do
      m = Manager.new(get_local_config)
      m.instance_variable_set("@message_bus", message_bus)
      m
    end

    it "should fetch handles from service gateways" do
      EM.run do
        handles = {}
        manager

        manager.fetch_handles
        EM.next_tick do
          Common.config.services.each do |service|
            handles[service] = {:"test_id_#{service}" => {}}

            resp = VCAP::Services::Internal::InstanceHandles.new
            resp.service_name = service
            resp.handles = handles[service]
            resp.available = true

            message_bus.respond_to_request("#{service}.#{Common::CHAN_GW_HANDLES}",
                                           resp.encode)
          end

          manager.instance_variable_get("@instance_handles").should eq handles
          EM.stop
        end
      end
    end

    it "should validate handles before updating them" do
      test_handles = {handle:{}}
      service_name = "test"

      manager.update_handles(service_name, test_handles, false)
      manager.instance_variable_get("@instance_handles").should eq({})

      manager.update_handles(service_name, nil, true)
      manager.instance_variable_get("@instance_handles").should eq({})
    end

  end
end
