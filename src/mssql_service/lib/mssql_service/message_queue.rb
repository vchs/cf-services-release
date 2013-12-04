# Copyright (c) 2013-2015 VMware, Inc.

require 'uri'
require 'cgi'
require 'redis'

require_relative './queue'
require_relative './json_coder'

module VCAP::Services::MSSQL::MessageQueue
  include VCAP::Services::MSSQL
  extend self

  attr_accessor :queue_name
  attr_writer :coder

  def enqueue(klass, options)
    queue = (klass.respond_to?(:select_queue) && klass.select_queue(options)) || queue_from_class(klass)
    push(queue, options)
  end

  def push(queue, item)
    queue(queue) << item
  end

  def queue(name)
    @queues[name.to_s]
  end

  def redis=(server)
    redis = connect(server)

    @queues = Hash.new do |h, name|
      h[name] = VCAP::Services::MSSQL::Queue.new("#{@queue_name}:q:#{name}", redis, coder)
    end

    redis
  end

  def connect(server)
    case server
    when String
      if(server['redis://'])
        redis = parse_redis_url(server)
      end
      redis
    when Redis
      server
    else
      raise ArgumentError, "Invalid Server: #{server.inspect}"
    end
  end

  # @param server [String] redis://:password@127.0.0.1:6379/1/msssql
  # @return [Redis]
  def parse_redis_url(server)
    uri = URI.parse(server)
    db, @queue_name = uri.path.split('/')[1, 2]
    password = CGI.unescape(uri.password) if uri.password

    redis = Redis.new(
      :host => uri.host,
      :port => uri.port,
      :password => password,
      :db => db,
      :thread_safe => true
    )
  end

  def coder
    @coder ||= JsonCoder.new
  end

  def queue_from_class(klass)
    if klass.instance_variable_defined?(:@queue)
      klass.instance_variable_get(:@queue)
    else
      (klass.respond_to?(:queue) and klass.queue)
    end
  end
end
