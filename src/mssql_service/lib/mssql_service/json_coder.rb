require 'json'

module VCAP::Services::MSSQL
  class EncodeException < StandardError; end
  class DecodeException < StandardError; end

  class Coder
    def encode(object)
      raise EncodeException
    end

    def dump(object)
      encode(object)
    end

    def decode(object)
      raise DecodeException
    end

    def load(object)
      decode(object)
    end
  end

  # The default coder for JSON serialization
  class JsonCoder < Coder
    def encode(object)
      JSON.dump object
    rescue JSON::GeneratorError => e
      raise EncodeException, e.message, e.backtrace
    end

    def decode(object)
      return unless object
      JSON.load object
    rescue JSON::ParserError => e
      raise DecodeException, e.message, e.backtrace
    end
  end
end
