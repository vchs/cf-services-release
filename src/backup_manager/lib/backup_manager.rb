require "ostruct"
require "cf_message_bus/message_bus"
require "steno"
require "vcap_services_messages"
require "vcap/common"
require "backup_manager/common"

module BackupManager
  class Manager
    include Common
    include VCAP::Services::Internal

    attr_reader :message_bus

    def initialize(options={})
      Common.config = OpenStruct.new(options)
      @log_counter = Steno::Sink::Counter.new
      setup_logging(Common.config.logging)
      @instance_handles = {}
    end

    def start
      EM.run do
        @messge_bus = CfMessageBus::MessageBus.new(uri: Common.config.mbus, logger: logger)
        fetch_handles
        setup_mbus_route
      end
    end

    def shutdown
      logger.info("Backup Manager is shutting down ...")
      EM.stop
      logger.info("bye")
    end

    def setup_mbus_route
      # TODO
    end

    def setup_logging(logging_config)
      steno_config = Steno::Config.to_config_hash(logging_config)
      steno_config[:context] = Steno::Context::ThreadLocal.new
      config = Steno::Config.new(steno_config)
      config.sinks << @log_counter
      Steno.init(config)
    end

    def fetch_handles
      logger.info("sending message to fetch handles")
      @fetch_handle_timers = {}
      Common.config.services.each do |service_name|
        @fetch_handle_timers[service_name] = EM.add_periodic_timer(Common.config.handle_fetch_interval) do
          fetch_handles_from_gw(service_name, "#{service_name}.#{CHAN_GW_HANDLES}")
        end
        EM.next_tick { fetch_handles_from_gw(service_name, "#{service_name}.#{CHAN_GW_HANDLES}") }
      end
    end

    def fetch_handles_from_gw(service_name, channel)
      logger.info("fetching handles through channel #{channel}")
      @message_bus.request(channel) do |resp|
        message = InstanceHandles.decode(resp)
        update_handles(service_name, message.handles)
      end
    end

    def update_handles(service_name, handles = {})
      return if handles.nil?
      logger.info("fetched #{handles.size} handles from #{service_name}")
      timer = @fetch_handle_timers[service_name] if @fetch_handle_timers
      EM.cancel_timer(timer) if timer

      unless @instance_handles.has_key? service_name
        @instance_handles[service_name] = VCAP.symbolize_keys handles
        EM.defer { setup_schedules(handles) }
      end
    end

    def setup_schedules(handles)
      handles.each do |handle|
        logger.info("setting up schedule for handle #{handle}")
        # TODO
      end
    end
  end
end
