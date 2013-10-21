# Copyright (c) 2009-2011 VMware, Inc.
require "mysql2"
require "monitor"

module VCAP
  module Services
    module Mysql
      module Util
        VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a

        def generate_credential(length=12)
          Array.new(length) { VALID_CREDENTIAL_CHARACTERS[rand(VALID_CREDENTIAL_CHARACTERS.length)] }.join
        end

        def make_logger
          return @logger if @logger
          @logger = Logger.new( STDOUT)
          @logger.level = Logger::DEBUG
          @logger
        end

        def fmt_error(e)
          "#{e}: [#{e.backtrace.join(" | ")}]"
        end

        def mysql_status(opts={})
          res = "ok"
          begin
            begin
              conn = Mysql2::Client.new(
                :host => opts[:host],
                :username => opts[:ins_user],
                :password => opts[:ins_pass],
                :database => opts[:db],
                :port => opts[:port],
                :socket => opts[:socket],
                :connect_timeout => 0.5
              )
            rescue Mysql2::Error => e
              # user had modified instance password, fallback to root account
              conn = Mysql2::Client.new(
                :host => opts[:host],
                :username => opts[:root_user],
                :password => opts[:root_pass],
                :database => opts[:db],
                :port => opts[:port],
                :socket => opts[:socket],
                :connect_timeout => 0.5
              )
              res = "password-modified"
            end
            conn.query("SHOW TABLES")
          ensure
            begin
              conn.close if conn
            rescue => e1
              #ignore
            end
          end
          res
        end

        def backup_mysql_server(type, mysql_config, mysqld_properties, dump_path, opts)
          raise ArgumentError, "Missing options." unless type && mysql_config &&
                                                         mysqld_properties && dump_path
          raise ArgumentError, "Unknown backup type" unless ["full", "incremental"].include? type
          make_logger
          host, user, password, port = %w{host user pass port}.map { |opt| mysql_config[opt] }

          cmd = ""
          path_variable = "$PATH"
          path_variable = "#{opts[:perl_bin]}:#{path_variable}" if opts[:perl_bin]
          path_variable = "#{opts[:xtrabackup_bin]}:#{path_variable}" if opts[:xtrabackup_bin]
          cmd << "export PATH=#{path_variable};"
          cmd << "export PERL5LIB=$PERL5LIB:#{opts[:dbd_mysql_lib]};" if opts[:dbd_mysql_lib]

          cmd << "innobackupex --host=#{host} --port=#{port} --user=#{user} --password=#{password} "

          defaults_file = "#{dump_path}/my.cnf"
          output_file = "#{dump_path}/backup_outpput"
          File.open(defaults_file, "w") do |f|
            f.write("[mysqld]\n")
            mysqld_properties.each { |k, v| f.write("#{k}=#{v}\n") }
          end

          if type == "incremental"
            raise "Cannot create incremental backup. Missing LSN." unless opts[:last_lsn]
            cmd << "--incremental --incremental-lsn=#{opts[:last_lsn]} "
          end

          cmd << "--defaults-file=#{defaults_file} #{dump_path} > #{output_file} 2>&1"
          @logger.info("Take backup command: #{cmd}")

          on_err = Proc.new do |command, code, msg|
            raise "CMD '#{command}' exit with code: #{code}. Message: #{msg}"
          end
          res = CMDHandle.execute(cmd, nil, on_err)

          raise "Failed to execute dump command to #{host}" unless res
          output = File.read(output_file)
          backup_folder = output.match(/Backup created in directory '(.+)'/)[1]
          last_lsn = output.match(/log scanned up to \((\d+)\)/)[1]
          raise "Can't get necessary data from backup output" unless backup_folder && last_lsn
          {:files => [backup_folder], :last_lsn => last_lsn }
        rescue => e
          @logger.error("Error backup server on host #{host}: #{fmt_error(e)}")
          nil
        ensure
          FileUtils.rm_rf(defaults_file, :secure => true)
          FileUtils.rm_rf(output_file, :secure => true)
        end

        # dump a single database to the given path
        #  db: the name of the database you want to dump
        #  mysql_config: hash contains following keys:
        #    host, port, user, password and socket as optional
        #  dump_file_path: full file path for dump file
        #  opts : other_options
        #    mysqldump_bin: path of mysqldump binary if not in PATH
        #    gzip_bin: path of gzip binary if not in PATH
        #
        def dump_database(db, mysql_config, dump_file_path, opts={})
          raise ArgumentError, "Missing options." unless db && mysql_config && dump_file_path
          make_logger
          host, user, password, port, socket =  %w{host user pass port socket}.map { |opt| mysql_config[opt] }
          mysql_dump_bin = opts[:mysqldump_bin] || "mysqldump"
          gzip_bin = opts[:gzip_bin] || "gzip"

          socket_str = "-S #{socket}"
          cmd = "#{mysql_dump_bin} -h#{host} --user='#{user}' --password='#{password}' -P#{port} #{socket_str if socket} -R --single-transaction #{db}| #{gzip_bin} - > #{dump_file_path}"
          @logger.info("Take snapshot command:#{cmd}")

          on_err = Proc.new do |command, code, msg|
            raise "CMD '#{command}' exit with code: #{code}. Message: #{msg}"
          end
          res = CMDHandle.execute(cmd, nil, on_err)
          res
        rescue => e
          @logger.error("Error dump db #{db}: #{fmt_error(e)}")
          nil
        end

        def handle_discarded_routines(db_name, connection)
          begin
            res = connection.query("select definer from mysql.proc where db='#{db_name}' and  security_type ='definer' and (type='PROCEDURE' OR type='FUNCTION') and definer NOT IN (select concat(user, '@', host) as valid_definer from mysql.user)")
            invalid_definers = []
            res.each do |invalid_routine|
              invalid_definers << "\"#{invalid_routine['definer']}\"";
            end
            if invalid_definers.count > 0
              connection.query("update mysql.proc set security_type='invoker' where db='#{db_name}' and security_type = 'definer' and (type='PROCEDURE' OR type='FUNCTION') and definer in (#{invalid_definers.join(',')})")
            end
          rescue => e
            @logger.error("Error to delete the routines in db #{db_name} with security_type is DEFINER but the definer does not exist: #{fmt_error(e)}")
          end
        end

        # import data from the dumpfile generated by dump_database
        #  db: the name of the database you want to import
        #  mysql_config: hash contains following keys:
        #    host, port, user, password (root account)and socket as optional
        #  dump_file_path: full file path for dump file
        #  opts : other_options
        #    mysql_bin: path of mysql binary if not in PATH
        #    gzip_bin: path of gzip binary if not in PATH
        #  import_user: the user account used to import db
        #  import_pass: the password used to import db
        def import_dumpfile(db, mysql_config, import_user, import_pass, dump_file_path, opts={})
          raise ArgumentError, "Missing options." unless db && mysql_config && dump_file_path
          make_logger
          host, user, password, port, socket =  %w{host user pass port socket}.map { |opt| mysql_config[opt] }
          mysql_bin = opts[:mysql_bin] || "mysql"
          gzip_bin = opts[:gzip_bin] || "gzip"

          @connection = Mysql2::Client.new(:host => host, :username => user, :password => password, :database => 'mysql' , :port => port.to_i, :socket => socket) unless @connection
          revoke_privileges(db)

          # rebuild database to remove all tables in old db.
          kill_database_session(@connection, db)
          @connection.query("DROP DATABASE #{db}")
          @connection.query("CREATE DATABASE #{db}")
          restore_privileges(db) if @connection

          socket_str = "-S #{socket}"
          cmd = "#{gzip_bin} -dc #{dump_file_path}| #{mysql_bin} -h#{host} -P#{port} --user='#{import_user}' --password='#{import_pass}' #{socket_str if socket} #{db}"
          @logger.info("import dump file cmd: #{cmd}")
          on_err = Proc.new do |command, code, msg|
            raise "CMD '#{command}' exit with code: #{code}. Message: #{msg}"
          end
          res = CMDHandle.execute(cmd, nil, on_err)
          handle_discarded_routines(db, @connection)
          res
        rescue => e
          @logger.error("Failed in import dumpfile to instance #{db}: #{fmt_error(e)}")
          nil
        ensure
          restore_privileges(db) if @connection
        end

        protected
        def revoke_privileges(name)
          @connection.query("UPDATE db SET insert_priv='N', create_priv='N', update_priv='N', lock_tables_priv='N' WHERE Db='#{name}'")
          @connection.query("FLUSH PRIVILEGES")
        end

        def restore_privileges(name)
          @connection.query("UPDATE db SET insert_priv='Y', create_priv='Y', update_priv='Y', lock_tables_priv='Y' WHERE Db='#{name}'")
          @connection.query("FLUSH PRIVILEGES")
        end

        def kill_database_session(connection, database)
          @logger.info("Kill all sessions connect to db: #{database}")
          process_list = connection.query("show processlist")
          process_list.each do |proc|
            thread_id, user, db, command, time, info = proc["Id"], proc["User"], proc["db"], proc["Command"], proc["Time"], proc["Info"]
            if (db == database) and (user != "root")
              connection.query("KILL #{thread_id}")
              @logger.info("Kill session: user:#{user} db:#{db}")
            end
          end
        end

        class Connection
          attr_accessor :checked_out_by
          attr_reader :conn
          attr_accessor :last_active_time

          def initialize(opts)
            @opts = opts
            @conn = Mysql2::Client.new(@opts)
            @last_active_time = Time.now
            @expire = opts[:expire]
          end

          def active?
            @conn && @conn.ping
          end

          def reconnect
            @conn.close if @conn
            @conn = Mysql2::Client.new(@opts)
          end

          # Verify the embedded mysql connection. Reconnect if necessary
          # close expired connection
          def verify!(check_expire)
            if check_expire && expire?
              close if @conn
            else
              reconnect unless active?
              self
            end
          end

          def expire?
            (Time.now - @last_active_time).to_i > @expire
          end

          def close
            @conn.close
            @conn = nil
          end
        end

        class ConnectionPool
          attr_reader  :timeout, :size
          include Util
          def initialize(options)
            @options = options
            @timeout = options[:wait_timeout] || 10
            @size = (options[:pool] && options[:pool].to_i) || 1
            @min = (options[:pool_min] && options[:pool_min].to_i) || 1
            @max = (options[:pool_max] && options[:pool_max].to_i) || 5
            @size = @size < @min ? @min : (@size > @max ? @max : @size)
            @options[:expire] ||= 300 #seconds
            @logger = options[:logger] || make_logger
            @connections = []
            @connections.extend(MonitorMixin)
            @cond = @connections.new_cond
            @reserved_connections = {}
            @checked_out = []
            @metrix_lock = Mutex.new
            @latency_sum = 0
            @queries_served = 1
            @worst_latency = 0
            @shutting_down = false
            for i in 1..@size do
              @connections << Connection.new(@options)
            end
          end

          def inspect
            {
              :size => @connections.size,
              :checked_out_size => @checked_out.size,
              :checked_out_by => @checked_out.map{|conn| conn.checked_out_by },
              :average_latency_ms => @latency_sum / @queries_served,
              :worst_latency_ms => @worst_latency
            }
          rescue => e
            @logger.warn("Error in inspect: #{e}")
            nil
          end

          def timing
            t1 = Time.now
            yield
          ensure
            update_latency_metric((Time.now - t1) * 1000)
          end

          def update_latency_metric(latency)
            @metrix_lock.synchronize {
              @latency_sum += latency
              @queries_served += 1
              @worst_latency = latency if latency > @worst_latency
            }
          end

          def parse_caller(callstack)
            frame = callstack[0]
            method = nil
            method = $1 if frame =~ /in `([^']+)/
            method
          end

          def with_connection
            connection_id = current_connection_id
            fresh_connection = !@reserved_connections.has_key?(connection_id)
            connection = (@reserved_connections[connection_id] ||= checkout)

            direct_caller = parse_caller(caller(1))
            connection.checked_out_by = direct_caller

            timing { yield connection.conn }
            connection.last_active_time = Time.now
          ensure
            release_connection(connection_id) if fresh_connection
          end

          # verify all pooled connections, remove the expired ones
          def keep_alive
            @connections.synchronize do
              (@connections - @checked_out).each do |conn|
                @connections.delete(conn) unless conn.verify!(@connections.size > @min)
              end
            end
            true
          end

          def close
            @connections.each do |conn|
              conn.close
            end
          end

          def shutdown
            @connections.synchronize do
              if @checked_out.size == 0
                close
                @connections.clear
              else
                @shutting_down = true
              end
            end
          end

          # Check the connction with mysql
          def connected?
            keep_alive
          rescue => e
            @logger.warn("Can't connection to mysql: [#{e.errno}] #{e.error}")
            nil
          end

          private
          def release_connection(with_id)
            conn = @reserved_connections.delete(with_id)
            checkin conn if conn
          end

          def clear_stale_cached_connections!
            keys = @reserved_connections.keys - Thread.list.find_all { |t|
              t.alive?
            }.map { |thread| thread.object_id }
            keys.each do |key|
              checkin @reserved_connections[key]
              @reserved_connections.delete(key)
            end
          end

          def checkout
            @connections.synchronize do
              raise "Mysql server at #{@options[:host]} is shutting down" if @shutting_down
              loop do
                if @checked_out.size < @connections.size
                  conn = (@connections - @checked_out).first
                  conn.verify!(false)
                  @checked_out << conn
                  return conn
                end

                if @connections.size < @max
                  conn = Connection.new(@options)
                  @connections << conn
                  @checked_out << conn
                  return conn
                end

                @cond.wait(@timeout)

                if @checked_out.size < @connections.size
                  next
                else
                  clear_stale_cached_connections!
                  if @checked_out.size == @connections.size
                    raise Mysql2::Error, "could not obtain a database connection within #{@timeout} seconds.  The max pool size is currently #{@max}; consider increasing it."
                  end
                end
              end
            end
          end

          def checkin(conn)
            @connections.synchronize do
              @checked_out.delete conn
              @cond.signal
            end
            shutdown if @shutting_down
          end

          def current_connection_id
            Thread.current.object_id
          end
        end
      end
    end
  end
end
