require 'yaml'
require 'yajl'
require 'ostruct'
require 'cf_message_bus/message_bus'
require 'steno'

require 'vcap/common'
require 'vcap/component'

require 'svc_hm/common'

module ServicesHealthManager
  class Manager
    include Common

    attr_reader :message_bus

    def initialize(options={})
      Common.config = OpenStruct.new(options)
      @log_counter = Steno::Sink::Counter.new
      setup_logging(Common.config.logging)
    end

    def start
      EM.run do
        @message_bus = CfMessageBus::MessageBus.new(uri: Common.config.mbus, logger: logger)
        setup_nats_route
      end
    end

    def shutdown
      logger.info('shutting down...')
      EM.stop
      logger.info('bye')
    end

    def setup_nats_route
      message_bus.subscribe Common::CHAN_HEARTBEAT do |message|
        process_heartbeat(message)
      end
    end

    def setup_logging(logging_config)
      steno_config = Steno::Config.to_config_hash(logging_config)
      steno_config[:context] = Steno::Context::ThreadLocal.new
      config = Steno::Config.new(steno_config)
      config.sinks << @log_counter
      Steno.init(config)
    end

    def process_heartbeat(message)
      # TODO process heartbeat
      logger.info("Receive HB: #{message}")
    end
  end
end
