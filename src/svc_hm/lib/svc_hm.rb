require 'yaml'
require 'yajl'
require 'ostruct'
require 'cf_message_bus/message_bus'
require 'steno'

require 'vcap/common'
require 'vcap/component'

require 'svc_hm/common'
require 'svc_hm/svc_instance_registry'

module ServicesHealthManager
  class Manager
    include Common

    attr_reader :message_bus

    def initialize(options={})
      Common.config = OpenStruct.new(options)
      @log_counter = Steno::Sink::Counter.new
      @instance_registry = InstanceRegistry.new
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
      logger.info("Receive HeartBeat: #{message}")
      message = VCAP.symbolize_keys(message)

      node_hash = {}
      node_hash[:node_type] = message[:node_type]
      node_hash[:node_id] = message[:node_id]
      node_hash[:node_ip] = message[:node_ip]
      message[:instances].each do |svc_name, state|
        instance = get_instance(svc_name)
        msg, payload = instance.process_heartbeat(node_hash, state)
        logger.info "svc_hm.instance.process_heartbeat: send #{msg}" unless msg.nil?
        message_bus.publish(msg, payload) unless msg.nil?
      end

    end

    def get_instance(svc_name, new_options = {})
      @instance_registry.get(svc_name.to_s, new_options)
    end

  end
end
