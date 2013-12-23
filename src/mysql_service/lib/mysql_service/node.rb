# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"
require "pp"

require "uuidtools"
require "mysql2"
require "open3"
require "thread"
require "uri"

module VCAP
  module Services
    module Mysql
      class Node < VCAP::Services::Base::Node
        class ProvisionedService
        end
        class WardenProvisionedService < VCAP::Services::Base::Warden::Service
        end
      end
    end
  end
end

require "mysql_service/common"
require "mysql_service/mysql2_timeout"
require "mysql_service/util"
require "mysql_service/storage_quota"
require "mysql_service/mysql_error"
require "mysql_service/transaction_killer"

class VCAP::Services::Mysql::Node

  KEEP_ALIVE_INTERVAL = 15
  STORAGE_QUOTA_INTERVAL = 1

  include VCAP::Services::Mysql::Util
  include VCAP::Services::Mysql::Common
  include VCAP::Services::Mysql

  def initialize(options)
    super(options)
    @use_warden = options[:use_warden]
    @use_warden = false unless @use_warden === true
    if @use_warden
      @logger.debug('using warden')
      require "mysql_service/with_warden"
      self.class.send(:include, VCAP::Services::Mysql::WithWarden)
      self.class.send(:include, VCAP::Services::Base::Utils)
      self.class.send(:include, VCAP::Services::Base::Warden::NodeUtils)
    else
      @logger.debug('not using warden')
      require "mysql_service/without_warden"
      self.class.send(:include, VCAP::Services::Mysql::WithoutWarden)
    end

    init_internal(options)

    @mysql_configs = options[:mysql]
    @connection_pool_size = options[:connection_pool_size]

    @max_db_size = options[:max_db_size] * 1024 * 1024
    @max_long_query = options[:max_long_query]
    @max_long_tx = options[:max_long_tx]
    @kill_long_tx = options[:kill_long_tx]
    @max_user_conns = options[:max_user_conns] || 0
    @gzip_bin = options[:gzip_bin]
    @delete_user_lock = Mutex.new
    @base_dir = options[:base_dir]
    @local_db = options[:local_db]

    @long_queries_killed = 0
    @long_tx_killed = 0
    @long_tx_count = 0
    @long_tx_ids = {}
    @statistics_lock = Mutex.new
    @provision_served = 0
    @binding_served = 0

    #locks
    @keep_alive_lock = Mutex.new
    @kill_long_queries_lock = Mutex.new
    @kill_long_transaction_lock = Mutex.new
    @enforce_quota_lock = Mutex.new

    @connection_wait_timeout = options[:connection_wait_timeout]
    Mysql2::Client.default_timeout = @connection_wait_timeout
    Mysql2::Client.logger = @logger
    @supported_versions = options[:supported_versions]
    mysqlProvisionedService.init(options)
    @transaction_killer = VCAP::Services::Mysql::TransactionKiller.build(options[:mysql_provider])
  end

  def service_instances
    mysqlProvisionedService.all
  end

  def pre_send_announcement
    FileUtils.mkdir_p(@base_dir) if @base_dir

    DataMapper.setup(:default, @local_db)
    DataMapper::auto_upgrade!

    pre_send_announcement_internal(@options)

    EM.add_periodic_timer(STORAGE_QUOTA_INTERVAL) { EM.defer { enforce_storage_quota } }

    keep_alive_interval = KEEP_ALIVE_INTERVAL
    keep_alive_interval = [keep_alive_interval, @connection_wait_timeout.to_f/2].min if @connection_wait_timeout
    EM.add_periodic_timer(keep_alive_interval) { EM.defer { mysql_keep_alive } }
    EM.add_periodic_timer(@max_long_query.to_f/2) { EM.defer { kill_long_queries } } if @max_long_query > 0
    if @max_long_tx > 0
      EM.add_periodic_timer(@max_long_tx.to_f/2) { EM.defer { kill_long_transaction } }
    else
      @logger.info("long transaction killer is disabled.")
    end

    @qps_last_updated = 0
    @queries_served = 0
    # initialize qps counter
    get_qps

    check_db_consistency
  end

  def self.mysqlProvisionedServiceClass(use_warden)
    if use_warden
      VCAP::Services::Mysql::Node::WardenProvisionedService
    else
      VCAP::Services::Mysql::Node::ProvisionedService
    end
  end

  def all_instances_list
    mysqlProvisionedService.all.map { |s| s.service_id }
  end

  def all_bindings_list
    res = []
    all_ins_users = mysqlProvisionedService.all.map { |s| s.user }
    each_connection_with_key_and_port do |connection, key, port|
      # we can't query plaintext password from mysql since it's encrypted.
      connection.query('select DISTINCT user.user,db from user, db where user.user = db.user and length(user.user) > 0').each do |entry|
        # Filter out the instances handles
        res << gen_credential(key, entry["db"], entry["user"], port) unless all_ins_users.include?(entry["user"])
      end
    end
    res
  rescue Mysql2::Error => e
    @logger.error("MySQL connection failed: [#{e.errno}] #{e.error}")
    []
  end

  def announcement
    @capacity_lock.synchronize do
      {
        :available_capacity => @capacity,
        :max_capacity => @max_capacity,
        :capacity_unit => capacity_unit,
        :host => get_host
      }
    end
  end

  def check_db_consistency()
    db_list = []
    missing_accounts =[]
    each_connection do |connection|
      connection.query('select db, user from db').each(:as => :array) { |row| db_list.push(row) }
    end
    mysqlProvisionedService.all.each do |service|
      account = service.name, service.user
      missing_accounts << account unless db_list.include?(account)
    end
    missing_accounts.each do |account|
      db, user = account
      @logger.warn("Node database inconsistent!!! db:user <#{db}:#{user}> not in mysql.")
    end
    missing_accounts
  rescue Mysql2::Error => e
    @logger.error("MySQL connection failed: [#{e.errno}] #{e.error}")
    nil
  end

  def mysql_connect(mysql_config, exit_on_fail = true)
    host, user, password, port, socket = %w{host user pass port socket}.map { |opt| mysql_config[opt] }

    5.times do
      begin
        return ConnectionPool.new(:host => host, :username => user, :password => password, :database => "mysql", :port => port.to_i, :socket => socket, :logger => @logger, :pool => @connection_pool_size["min"], :pool_min => @connection_pool_size["min"], :pool_max => @connection_pool_size["max"])
      rescue Mysql2::Error => e
        @logger.warn("MySQL connection attempt failed: [#{e.errno}] #{e.error}")
        sleep(1)
      end
    end

    @logger.fatal("MySQL connection to #{host} unrecoverable")
    if exit_on_fail
      shutdown
      exit
    end
  end

  def node_ready?()
    mysqlProvisionedService.all.each do |instance|
      conn_pool = fetch_pool(instance.service_id)
      return false unless conn_pool && conn_pool.connected?
    end
    true
  end

  #keep connection alive, and check db liveness
  def mysql_keep_alive
    acquired = @keep_alive_lock.try_lock
    return unless acquired
    5.times do
      begin
        each_pool { |conn_pool| conn_pool.keep_alive }
        return
      rescue Mysql2::Error => e
        @logger.error("MySQL connection attempt failed: [#{e.errno}] #{e.error}")
        sleep(5)
      end
    end

    unless @use_warden
      @logger.fatal("MySQL connection unrecoverable")
      shutdown
      exit
    end
  ensure
    @keep_alive_lock.unlock if acquired
  end

  def kill_long_queries
    acquired = @kill_long_queries_lock.try_lock
    return unless acquired
    each_connection do |connection|
      process_list = connection.query("show processlist")
      process_list.each do |proc|
        thread_id, user, db, command, time, info, state = %w(Id User db Command Time Info State).map { |o| proc[o] }
        if (time.to_i >= @max_long_query) and (command == 'Query') and (user != 'root') then
          connection.query("KILL QUERY #{thread_id}")
          @logger.warn("Killed long query: user:#{user} db:#{db} time:#{time} state: #{state} info:#{info}")
          @long_queries_killed += 1
        end
      end
    end
  rescue Mysql2::Error => e
    @logger.error("MySQL error: [#{e.errno}] #{e.error}")
  ensure
    @kill_long_queries_lock.unlock if acquired
  end

  def kill_long_transaction
    acquired = @kill_long_transaction_lock.try_lock
    return unless acquired

    query_str = <<-QUERY
      SELECT * from (
        SELECT trx_started, id, user, db, trx_query, TIME_TO_SEC(TIMEDIFF(NOW() , trx_started )) as active_time
        FROM information_schema.INNODB_TRX t inner join information_schema.PROCESSLIST p
        ON t.trx_mysql_thread_id = p.ID
        WHERE trx_state='RUNNING' and user!='root'
      ) as inner_table
      WHERE inner_table.active_time > #{@max_long_tx}
    QUERY

    each_connection_with_key do |connection, key|
      result = connection.query(query_str)
      current_long_tx_ids = []
      @long_tx_ids[key] = [] if @long_tx_ids[key].nil?
      result.each do |trx|
        trx_started, id, user, db, trx_query, active_time = %w(trx_started id user db trx_query active_time).map { |o| trx[o] }
        if @kill_long_tx
          @transaction_killer.kill(id, connection)
          @logger.warn("Kill long transaction: user:#{user} db:#{db} thread:#{id} trx_query:#{trx_query} active_time:#{active_time}")
          @long_tx_killed += 1
        else
          @logger.warn("Log but not kill long transaction: user:#{user} db:#{db} thread:#{id} trx_query:#{trx_query} active_time:#{active_time}")
          current_long_tx_ids << id
          unless @long_tx_ids[key].include?(id)
            @long_tx_count += 1
          end
        end
      end
      @long_tx_ids[key] = current_long_tx_ids
    end
  rescue => e
    @logger.error("Error during kill long transaction: #{e}.")
  ensure
    @kill_long_transaction_lock.unlock if acquired
  end

  def provision(plan, credential, version=nil, properties={})
    raise MysqlError.new(MysqlError::MYSQL_INVALID_PLAN, plan) unless plan == @plan
    raise ServiceError.new(ServiceError::UNSUPPORTED_VERSION, version) unless @supported_versions.include?(version)

    raise ServiceError.new(ServiceError::NO_CREDENTIAL) unless credential

    provisioned_service = nil
    tried_free_port = false
    port = nil
    begin
      # name: the database name
      service_id, name, user, password = %w(service_id name user password).map { |key| credential[key] }
      port = new_port(credential["port"])
      provisioned_service = mysqlProvisionedService.create(port, service_id, name, user, version, properties)

      is_restoring = properties["is_restoring"] rescue nil
      provisioned_service.run do |instance|
        setup_pool(instance)
        raise "Could not create database" unless is_restoring || create_database(instance, password)
      end
      response = gen_credential(provisioned_service.service_id, provisioned_service.name, provisioned_service.user, get_port(provisioned_service))
      @statistics_lock.synchronize do
        @provision_served += 1
      end
      return response
    rescue => e
      if e.is_a?(ServiceError) && e.error_code == ServiceError::PORT_IN_USE[0]
        raise if tried_free_port
        port = credential["port"]
        @logger.warn("Found an occupied port #{port}, " \
                     "try to delete instances with this port")
        ids = mysqlProvisionedService.all(:port => port).map{ |ins| ins.service_id }
        ids.each{ |id| unprovision(id, []) }
        tried_free_port = true
        retry
      end

      handle_provision_exception(provisioned_service)
      raise e
    end
  end

  def unprovision(service_id, credentials)
    return if service_id.nil?
    @logger.debug("Unprovision database:#{service_id} and its #{credentials.size} bindings")
    provisioned_service = mysqlProvisionedService.get(service_id)
    raise MysqlError.new(MysqlError::MYSQL_CONFIG_NOT_FOUND, service_id) if provisioned_service.nil?
    # Delete all bindings, ignore not_found error since we are unprovision
    begin
      credentials.each { |credential| unbind(credential) } if credentials
    rescue => e
      # ignore error, only log it
      @logger.warn("Error found in unbind operation:#{e}")
    end
    delete_database(provisioned_service)

    help_unprovision(provisioned_service)
    @logger.debug("Successfully fulfilled unprovision request: #{service_id}")
    true
  end

  #FIXME: accept user input password
  def bind(service_id, bind_options, credential=nil)
    @logger.debug("Bind service for instance:#{service_id}, bind_options = #{bind_options}")
    binding = nil
    begin
      service = mysqlProvisionedService.get(service_id)
      raise MysqlError.new(MysqlError::MYSQL_CONFIG_NOT_FOUND, service_id) unless service
      # create new credential for binding
      binding = Hash.new
      if credential
        binding[:user] = credential["user"]
        binding[:password] = credential["password"]
      else
        binding[:user] = 'u' + generate_credential
        binding[:password] = 'p' + generate_credential
      end
      binding[:bind_options] = bind_options

      begin
        create_or_update_database_user(service_id, service.name, binding[:user], binding[:password], binding[:bind_options])
        enforce_instance_storage_quota(service)
      rescue Mysql2::Error => e
        raise "Could not create database user: [#{e.errno}] #{e.error}"
      end

      response = gen_credential(service_id, service.name, binding[:user], get_port(service))
      @logger.debug("Bind response: #{response.inspect}")
      @statistics_lock.synchronize do
        @binding_served += 1
      end
      return response
    rescue => e
      delete_database_user(binding[:user], service_id) if binding
      raise e
    end
  end

  def unbind(credential)
    return if credential.nil?
    @logger.debug("Unbind service: #{credential.inspect}")
    service_id, name, user = %w(service_id name user).map { |k| credential[k] }

    # validate the existence of credential, in case we delete a normal account because of a malformed credential
    conn_pool = fetch_pool(service_id)
    if conn_pool.nil?
      @logger.error("fail to fetch connection pool for #{credential.inspect}")
      return
    end
    conn_pool.with_connection do |connection|
      res = connection.query("SELECT * from mysql.user WHERE user='#{user}'")
      raise MysqlError.new(MysqlError::MYSQL_CRED_NOT_FOUND, credential.inspect) if res.count() <= 0
    end
    delete_database_user(user, service_id)
    conn_pool.with_connection do |connection|
      handle_discarded_routines(name, connection)
    end
    true
  end

  def create_database(provisioned_service, password)
    service_id, name, user = [:service_id, :name, :user].map { |field| provisioned_service.send(field) }

    begin
      start = Time.now
      @logger.debug("Creating: #{provisioned_service.inspect}")
      fetch_pool(service_id).with_connection do |connection|
        connection.query("CREATE DATABASE #{name}")
      end
      create_or_update_database_user(service_id, name, user, password, {"privileges" => ["FULL"]})
      @logger.debug("Done creating #{provisioned_service.inspect}. Took #{Time.now - start}.")
      return true
    rescue Mysql2::Error => e
      @logger.warn("Could not create database: [#{e.errno}] #{e.error}")
      return false
    end
  end

  def update_credentials(service_id, credentials)
    @logger.debug("Update credentials for instance:#{service_id}")

    raise ServiceError.new(ServiceError::NO_CREDENTIAL) unless credentials && credentials.is_a?(Hash) && credentials['password']

    password = credentials['password']

    service = mysqlProvisionedService.get(service_id)
    raise MysqlError.new(MysqlError::MYSQL_CONFIG_NOT_FOUND, service_id) unless service

    create_or_update_database_user(service_id, service.name, service.user, password)
  end

  def create_or_update_database_user(service_id, database, username, password , binding_options={"privileges"=>["FULL"]})
    @logger.info("Creating/Updating credentials: #{username} for instance #{service_id}")
    raise "Invalid binding options format #{binding_options.inspect}" unless binding_options.kind_of?(Hash) && binding_options["privileges"]
    binding_privileges = binding_options["privileges"]
    raise "Invalid binding privileges type #{binding_privileges.class}" unless binding_privileges.kind_of?(Array)

    fetch_pool(service_id).with_connection do |connection|
      escaped_password = connection.escape(password)
      grant = { "FULL" => "ALL", "READ_ONLY" => "SELECT" }
      binding_privileges.each do |privilege|
        ['%', 'localhost'].each do |host|
          raise "Unknown binding privileges #{privilege} for database #{database}, username #{username}, password #{password}" unless grant[privilege]
          connection.query("GRANT #{grant[privilege]} ON #{database}.* to #{username}@'#{host}' IDENTIFIED BY '#{escaped_password}' WITH MAX_USER_CONNECTIONS #{@max_user_conns}")
        end
      end
      connection.query("FLUSH PRIVILEGES")
    end
  end

  def delete_database(provisioned_service)
    service_id, name, user = [:service_id, :name, :user].map { |field| provisioned_service.send(field) }
    begin
      delete_database_user(user, service_id)
      @logger.info("Deleting database: #{name}")
      fetch_pool(service_id).with_connection do |connection|
        connection.query("DROP DATABASE #{name}")
      end
    rescue Mysql2::Error => e
      @logger.error("Could not delete database: [#{e.errno}] #{e.error}")
    end
  end

  def delete_database_user(user, service_id)
    @logger.info("Delete user #{user}")
    @delete_user_lock.synchronize do
      ["%", "localhost"].each do |host|
        fetch_pool(service_id).with_connection do |connection|
          res = connection.query("SELECT user from mysql.user where user='#{user}' and host='#{host}'")
          if res.count == 1
            connection.query("DROP USER #{user}@'#{host}'")
          else
            @logger.warn("Failure to delete non-existent user #{user}@'#{host}'")
          end
        end
      end
      kill_user_session(user, service_id)
    end
  rescue Mysql2::Error => e
    @logger.error("Could not delete user '#{user}': [#{e.errno}] #{e.error}")
  end

  def kill_user_session(user, service_id)
    @logger.info("Kill sessions of user: #{user}")
    begin
      fetch_pool(service_id).with_connection do |connection|
        process_list = connection.query("show processlist")
        process_list.each do |proc|
          thread_id, user_, db = proc["Id"], proc["User"], proc["db"]
          if user_ == user then
            connection.query("KILL #{thread_id}")
            @logger.info("Kill session: user:#{user} db:#{db}")
          end
        end
      end
    rescue Mysql2::Error => e
      # kill session failed error, only log it.
      @logger.error("Could not kill user session.:[#{e.errno}] #{e.error}")
    end
  end

  def instance_configs instance
    return unless instance
    config = @mysql_configs[instance.version]
    result = %w{host user pass port socket mysql_bin mysqldump_bin}.map { |opt| config[opt] }
    result[0] = instance.ip if @use_warden

    result
  end

  def varz_details
    varz = super
    # how many queries served since startup
    varz[:queries_since_startup] = get_queries_status
    # queries per second
    varz[:queries_per_second] = get_qps
    # disk usage per instance
    status = get_instance_status
    varz[:database_status] = status
    varz[:max_capacity] = @max_capacity
    varz[:available_capacity] = @capacity
    varz[:used_capacity] = @max_capacity - @capacity
    # how many long queries and long txs are killed.
    varz[:long_queries_killed] = @long_queries_killed
    varz[:long_transactions_killed] = @long_tx_killed
    varz[:long_transactions_count] = @long_tx_count #logged but not killed
    # how many provision/binding operations since startup.
    @statistics_lock.synchronize do
      varz[:provision_served] = @provision_served
      varz[:binding_served] = @binding_served
    end
    # provisioned services status
    varz[:instances] = {}
    begin
      mysqlProvisionedService.all.each do |instance|
        varz[:instances][instance.service_id.to_sym] = get_status(instance)
      end
    rescue => e
      @logger.error("Error get instance list: #{e}")
    end
    # connection pool information
    varz[:pools] = {}
    each_pool_with_key { |conn_pool, key| varz[:pools][key] = conn_pool.inspect }
    varz
  rescue => e
    @logger.error("Error during generate varz: #{e}")
    {}
  end

  def get_status(instance)
    res = "ok"
    host, root_user, root_pass, port, socket = instance_configs(instance)

    begin
      res = mysql_status(
        :host => host,
        :root_user => root_user,
        :root_pass => root_pass,
        :db => instance.name,
        :port => port.to_i,
        :socket => socket,
      )
    rescue => e
      @logger.warn("Error get status of #{instance.service_id}: #{e}")
      res = "fail"
    end

    res
  end

  def get_instance_health(service_id)
    instance = mysqlProvisionedService.get(service_id)
    health = instance.nil? ? 'fail' : get_status(instance)
    { :health => health }
  end

  def get_queries_status()
    total = 0
    each_connection do |connection|
      result = connection.query("SHOW STATUS WHERE Variable_name ='QUERIES'")
      total += result.to_a[0]["Value"].to_i if result.count != 0
    end
    total
  end

  def get_qps()
    queries = get_queries_status
    ts = Time.now.to_i
    delta_t = (ts - @qps_last_updated).to_f
    qps = (queries - @queries_served)/delta_t
    @queries_served = queries
    @qps_last_updated = ts
    qps
  rescue Mysql2::Error => e
    @logger.error("MySQL connection failed: [#{e.errno}] #{e.error}")
    0
  end

  def get_instance_status()
    total = []

    each_connection_with_key do |connection, service_id|
      all_dbs = []
      result = connection.query('show databases')
      result.each { |db| all_dbs << db["Database"] }
      system_dbs = ['mysql', 'information_schema']
      sizes = connection.query(
        'SELECT table_schema "name",
        sum( data_length + index_length ) "size"
        FROM information_schema.TABLES
        GROUP BY table_schema')
      result = []
      db_with_tables = []
      sizes.each do |i|
        db = {}
        name, size = i["name"], i["size"]
        next if system_dbs.include?(name)
        db_with_tables << name
        db[:service_id] = service_id
        db[:name] = name
        db[:size] = size.to_i
        db[:max_size] = @max_db_size
        result << db
      end
      # handle empty db without table
      (all_dbs - db_with_tables - system_dbs).each do |db|
        result << {:service_id => service_id, :name => db, :size => 0, :max_size => @max_db_size}
      end
      total += result
    end
    total
  end

  def gen_credential(service_id, database, username, port)
    host = get_host

    {
      "service_id" => service_id,
      "name" => database,
      "hostname" => host,
      "host" => host,
      "port" => port,
      "user" => username,
      "username" => username,
      "uri" => generate_uri(username, host, port, database),
    }
  end

  def get_host
    return @host if @host
    host = @mysql_configs.values.first['host']
    if ['localhost', '127.0.0.1'].include?(host)
      host = super
    end
    @host = host
    @host
  end

  def each_connection
    each_connection_with_identifier { |conn, identifier| yield conn }
  end

  def each_connection_with_key_and_port
    each_connection_with_identifier do |conn, identifier|
      yield conn, extract_attr(identifier, :key), extract_attr(identifier, :port)
    end
  end

  def each_connection_with_key
    each_connection_with_identifier { |conn, identifier| yield conn, extract_attr(identifier, :key) }
  end

  def each_pool
    each_pool_with_identifier { |conn_pool, identifier| yield conn_pool }
  end

  def each_pool_with_key
    each_pool_with_identifier { |conn_pool, identifier| yield conn_pool, extract_attr(identifier, :key) }
  end

  def each_connection_with_identifier
    each_pool_with_identifier do |conn_pool, identifier|
      begin
        conn_pool.with_connection { |conn| yield conn, identifier }
      rescue => e
        @logger.warn("with_connection failed: #{fmt_error(e)}")
      end
    end
  end

  private

  def generate_uri(username, host, port, database)
    scheme = 'mysql'
    credentials = "#{username}"
    path = "/#{database}"

    uri = URI::Generic.new(scheme, credentials, host, port, nil, path, nil, nil, nil)
    uri.to_s
  end
end

class VCAP::Services::Mysql::Node::ProvisionedService
  include DataMapper::Resource
  property :service_id, String, :key => true
  property :name, String, :required => true
  property :user, String, :required => true
  property :plan, Integer, :required => true
  property :quota_exceeded, Boolean, :default => false
  property :version, String

  class << self
    # non-wardenized mysql does not support "properties"
    def create(port, service_id, name, user, version, properties={})
      provisioned_service = new
      provisioned_service.service_id = service_id
      provisioned_service.name = name
      provisioned_service.user = user
      provisioned_service.plan = 1
      provisioned_service.version = version
      provisioned_service
    end

    #no-ops methods
    def method_missing(method_name, *args, &block)
      no_ops = [:init]
      super unless no_ops.include?(method_name)
    end
  end

  def run
    yield self if block_given?
    save
  end
end

class VCAP::Services::Mysql::Node::WardenProvisionedService

  include DataMapper::Resource
  include VCAP::Services::Mysql::Util

  property :service_id, String, :key => true
  property :name, String, :required => true
  property :port, Integer, :unique => true
  property :user, String, :required => true
  property :plan, Integer, :required => true
  property :quota_exceeded, Boolean, :default => false
  property :container, String
  property :ip, String
  property :version, String

  private_class_method :new

  class << self
    def create(port, service_id, name, user, version, properties={})
      raise "Parameter missing" unless port
      provisioned_service = new
      provisioned_service.service_id = service_id
      provisioned_service.name = name
      provisioned_service.port = port
      provisioned_service.user = user
      provisioned_service.plan = 1
      provisioned_service.version = version

      opts = {}
      opts[:remove_base_dir] = false if properties && properties["is_restoring"]
      provisioned_service.prepare_filesystem(@max_disk, opts)
      FileUtils.mkdir_p(provisioned_service.tmp_dir)
      provisioned_service
    end

    def options
      @@options
    end
  end

  def service_port
    case version
    when "5.5"
      3307
    when "5.6"
      3308
    else
      3306
    end
  end

  def service_conf
    case version
    when "5.5"
      "my55.cnf"
    when "5.6"
      "my56.cnf"
    else
      "my.cnf"
    end
  end

  ["start", "stop", "status"].each do |op|
    define_method "#{op}_script".to_sym do
      passwd = @@options[:mysql][version]["pass"]
      "#{service_script} #{op} /var/vcap/sys/run/mysqld /var/vcap/sys/log/mysql #{common_dir} #{bin_dir} /var/vcap/store/mysql #{version} #{passwd}"
    end
  end

  def tmp_dir
    File.join(base_dir, "tmp")
  end

  def start_options
    options = super
    options[:start_script] = {:script => start_script, :use_spawn => true}
    options[:service_port] = service_port
    update_bind_dirs(options[:bind_dirs], {:src => base_dir}, {:src => base_dir, :dst => "/var/vcap/sys/run/mysqld"})
    update_bind_dirs(options[:bind_dirs], {:src => log_dir}, {:src => log_dir, :dst => "/var/vcap/sys/log/mysql"})
    options[:bind_dirs] << {:src => data_dir, :dst => "/var/vcap/store/mysql"}
    options[:bind_dirs] << {:src => tmp_dir, :dst => "/var/vcap/data/mysql_tmp"}
    options
  end

  def stop_options
    options = super
    options[:stop_script] = {:script => stop_script}
    options
  end

  def status_options
    options = super
    options[:status_script] = {:script => status_script}
    options
  end

  def finish_start?
    # Mysql does this in "setup_pool" function, so just return true here
    true
  end

  def running?
    res = true
    host = self[:ip]
    db = self[:name]
    mysql_configs = self.class.options[:mysql][self[:version]]
    root_user = mysql_configs["user"]
    root_pass = mysql_configs["pass"]
    port = mysql_configs["port"].to_i
    socket = mysql_configs["socket"]

    begin
      mysql_status(
        :host => host,
        :root_user => root_user,
        :root_pass => root_pass,
        :db => db,
        :port => port,
        :socket => socket,
      )
    rescue
      res = false
    end

    res
  end
end
