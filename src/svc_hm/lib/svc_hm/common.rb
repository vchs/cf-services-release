module ServicesHealthManager
  module Common
    CHAN_HEARTBEAT = "svc.heartbeat"
    @config = {}
    class << self
      attr_accessor :config
    end

    def logger
      @logger ||= Steno.logger("svc_hm")
    end
  end
end

