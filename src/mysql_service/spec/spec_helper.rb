# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

# Bundler.require(:test)

SPEC_ROOT = File.expand_path(File.dirname(__FILE__))
def require_dir(dir_pattern)
  Dir.glob(File.expand_path(dir_pattern, File.dirname(__FILE__))) do |filename|
    require filename
  end
end


require_dir '../tmp/integration-test-support/support/**/*.rb'
require_dir 'support/**/*.rb'

tmp_dir = File.expand_path('../tmp', File.dirname(__FILE__))
FileUtils.mkdir_p(tmp_dir)
IntegrationExampleGroup.tmp_dir = tmp_dir

SPEC_TMP_DIR = "/tmp/mysql_node_spec"
REDIS_PID = "#{SPEC_TMP_DIR}/redis.pid"
REDIS_CACHE_PATH = "#{SPEC_TMP_DIR}/redis_cache"
FileUtils.mkdir_p(REDIS_CACHE_PATH)

RSpec.configure do |c|
  c.include IntegrationExampleGroup, :type => :integration,
    :example_group => {:file_path => /\/integration\//}
  c.include IntegrationExampleGroup, :type => :integration,
    :example_group => {:file_path => /\/functional\//}
end

require 'rubygems'
require 'rspec'
require 'bundler/setup'
require 'vcap_services_base'
require 'mysql_service/util'
require 'mysql_service/provisioner'
require 'mysql_service/node'
require 'sequel'

require 'mysql_service/with_warden'
# monkey patch of wardenized node
module VCAP::Services::Mysql::WithWarden
  alias_method :pre_send_announcement_internal_ori, :pre_send_announcement_internal
  def pre_send_announcement_internal(options)
    unless @options[:not_start_instances]
      pre_send_announcement_internal_ori(options)
    else
      @pool_mutex = Mutex.new
      @pools = {}
      @logger.info("Not to start instances")
      mysqlProvisionedService.all.each do |instance|
        new_port(instance.port)
        setup_pool(instance)
      end
    end
  end

  def create_missing_pools
    mysqlProvisionedService.all.each do |instance|
      unless @pools.keys.include?(instance.service_id)
        new_port(instance.port)
        setup_pool(instance)
      end
    end
  end

  alias_method :shutdown_ori, :shutdown
  def shutdown
    if @use_warden && @options[:not_start_instances]
      super
    else
      shutdown_ori
    end
  end
end

module Boolean; end
class ::TrueClass; include Boolean; end
class ::FalseClass; include Boolean; end

def getLogger()
  logger = Logger.new(STDOUT)
  logger.level = Logger::ERROR
  return logger
end

def connect_to_mysql(creds, password)
  host, user, port, db =  %w{hostname user port name}.map { |opt| creds[opt] }
  Mysql2::Client.new(:host => host, :username => user, :password => password, :database => db, :port => port)
end

def connection_pool_klass
    VCAP::Services::Mysql::Util::ConnectionPool
end

def parse_property(hash, key, type, options = {})
  obj = hash[key]
  if obj.nil?
    raise "Missing required option: #{key}" unless options[:optional]
    nil
  elsif type == Range
    raise "Invalid Range object: #{obj}" unless obj.kind_of?(Hash)
    first, last = obj["first"], obj["last"]
    raise "Invalid Range object: #{obj}" unless first.kind_of?(Integer) and last.kind_of?(Integer)
    Range.new(first, last)
  else
    raise "Invalid #{type} object: #{obj}" unless obj.kind_of?(type)
    obj
  end
end

def config_base_dir
  File.join(File.dirname(__FILE__), '..', 'config')
end

def getNodeTestConfig()
  config_file = File.join(config_base_dir, 'mysql_node.yml')
  config = YAML.load_file(config_file)
  options = {
    # service node related configs
    :logger             => getLogger,
    :node_tmp_dir       => SPEC_TMP_DIR,
    :plan               => parse_property(config, "plan", String),
    :capacity           => parse_property(config, "capacity", Integer),
    :gzip_bin           => parse_property(config, "gzip_bin", String),
    :node_id            => parse_property(config, "node_id", String),
    :mbus               => parse_property(config, "mbus", String),
    :ip_route           => parse_property(config, "ip_route", String, :optional => true),
    :use_warden         => parse_property(config, "use_warden", Boolean),
    :supported_versions => parse_property(config, "supported_versions", Array),
    :default_version    => parse_property(config, "default_version", String),

    # service instance related configs
    :mysql                   => parse_property(config, "mysql", Hash),
    :max_db_size             => parse_property(config, "max_db_size", Integer),
    :max_long_query          => parse_property(config, "max_long_query", Integer),
    :connection_pool_size    => parse_property(config, "connection_pool_size", Hash),
    :max_long_tx             => parse_property(config, "max_long_tx", Integer),
    :kill_long_tx            => parse_property(config, "kill_long_tx", Boolean),
    :max_user_conns          => parse_property(config, "max_user_conns", Integer, :optional => true),
    :connection_wait_timeout => 10,
    :max_disk                => parse_property(config, "max_disk", Integer),

    # hard code unit test directories of mysql unit test to /tmp
    :base_dir => File.join(SPEC_TMP_DIR, "data"),
    :local_db => File.join("sqlite3:", SPEC_TMP_DIR, "mysql_node.db"),
    :disabled_file => File.join(SPEC_TMP_DIR, "DISABLED"),
  }
  if options[:use_warden]
    warden_config = parse_property(config, "warden", Hash, :optional => true)

    options[:service_log_dir]    = File.join(SPEC_TMP_DIR, "log")
    options[:service_bin_dir]    = parse_property(warden_config, "service_bin_dir", Hash)
    options[:service_common_dir] = parse_property(warden_config, "service_common_dir", String)

    options[:port_range] = parse_property(warden_config, "port_range", Range)
    options[:service_start_timeout] = parse_property(warden_config, "service_start_timeout", Integer, :optional => true, :default => 3)
    options[:filesystem_quota] = parse_property(warden_config, "filesystem_quota", Boolean, :optional => true)

    # hardcode the directories for mysql unit tests to /tmp
    options[:service_log_dir] = File.join(SPEC_TMP_DIR, "log")
    options[:image_dir]       = File.join(SPEC_TMP_DIR, "image_dir")
  else
    options[:ip_route] = "127.0.0.1"
  end
  options
end

def getProvisionerTestConfig()
  config_file = File.join(config_base_dir, 'mysql_gateway.yml')
  config = YAML.load_file(config_file)
  config = VCAP.symbolize_keys(config)
  options = {
    :logger   => getLogger,
    :version  => config[:service][:version],
    :local_ip => config[:host],
    :plan_management => config[:plan_management],
    :mbus => config[:mbus],
    :cc_api_version => "v1",
  }
  options
end

def get_worker_config()
  config_file = File.join(config_base_dir, 'mysql_worker.yml')
  config = YAML.load_file(config_file)
  config["mysqld"].each { |k, v| config["mysqld"][k]["datadir"] = File.join(SPEC_TMP_DIR, "data") }
  config["local_db"] = File.join("sqlite3:", SPEC_TMP_DIR, "mysql_node.db")
  ENV["WORKER_CONFIG"] = Yajl::Encoder.encode(config)
  config
end

def new_node(options)
  opts = options.dup
  opts[:not_start_instances] = true if opts[:use_warden]
  VCAP::Services::Mysql::Node.new(opts)
end

def start_redis
  redis_options = {
    "daemonize"     => 'yes',
    "pidfile"       => REDIS_PID,
    "port"          => get_worker_config["resque"]["port"],
    "timeout"       => 300,
    "dbfilename"    => "dump.rdb",
    "dir"           => REDIS_CACHE_PATH,
    "loglevel"      => "debug",
    "logfile"       => "stdout"
  }.map { |k, v| "#{k} #{v}" }.join("\n")
  `echo '#{redis_options}' | redis-server -`
end

def stop_redis
  %x{
    cat #{REDIS_PID} | xargs kill -QUIT
    rm -rf #{REDIS_CACHE_PATH}
  }
end

def expect_statement_allowed!(sql, options={})
  lastex = nil
  100.times do
    begin
      Sequel.connect(options) do |conn|
        sleep 0.1
        conn.run(sql)
      end
      return true
    rescue => e
      lastex = e
      # ignore
    end
  end
  raise "Timed out waiting for #{sql} to be allowed, " \
        "last exception #{lastex.inspect}"
end

def expect_statement_denied!(conn_string, sql)
  expect do
    100.times do
      Sequel.connect(conn_string) do |conn|
        sleep 0.1
        conn.run(sql)
      end
    end
  end.to raise_error(/command denied/)
end
