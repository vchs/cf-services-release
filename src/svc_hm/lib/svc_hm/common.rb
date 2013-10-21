require 'svc_hm/common'

module ServicesHealthManager
  module Common
    CHAN_HEARTBEAT = "svc.heartbeat".freeze

    TIMEOUT_PEER_LOST = 60

    @config = {}
    class << self
      attr_accessor :config
    end

    def logger
      @logger ||= Steno.logger("svc_hm")
    end


    def encode_json(obj)
      Yajl::Encoder.encode(obj)
    end

    def parse_json(string)
      Yajl::Parser.parse(string)
    end

    def now
      Time.now.to_i
    end

    def timestamp_older_than?(timestamp, age)
      timestamp > 0 && (now - timestamp) > age
    end

    def timestamp_fresher_than?(timestamp, age)
      timestamp > 0 && now - timestamp < age
    end

    def parse_utc(time)
      Time.parse(time).to_i
    end

  end
end

