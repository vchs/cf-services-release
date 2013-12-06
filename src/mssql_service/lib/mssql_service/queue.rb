# Copyright (c) 2013-2015 VMware, Inc.

require_relative './util'

module VCAP::Services::MSSQL
  class Queue
    include VCAP::Services::MSSQL::Util

    attr_reader :name

    def initialize name, redis, coder = Marshal
      @name  = name
      @redis = redis
      @coder = coder
    end

    def push object
      begin
        encoded_object = encode(object)
      rescue => e
        log "Invalid UTF-8 character in task: #{e.message}"
        return
      end

      log "name: #{@name}, msg: #{encoded_object}"
      @redis.lpush @name, encoded_object
    end

    alias :<< :push
    alias :enq :push

    def encode object
      @coder.dump object
    end

    def decode object
      @coder.load object
    end
  end
end
