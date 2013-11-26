# Copyright (c) 2009-2011 VMware, Inc.
require "mysql2"
require "monitor"

module VCAP
  module Services
    module Mysql
      module Util
        VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a

        PASSWORD_LENGTH = 9
        DBNAME_LENGTH = 9

        def password_length
          PASSWORD_LENGTH
        end

        def dbname_length
          DBNAME_LENGTH
        end

        def generate_service_id
          SecureRandom.uuid
        end

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
            rescue Mysql2::Error
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
            rescue
              #ignore
            end
          end
          res
        end

        def prepare_env(opts)
          env = {}
          path_variable = (ENV["PATH"] && ENV["PATH"].dup) || ""
          path_change = ""
          path_change = "#{opts[:perl_bin]}:#{path_change}" if opts[:perl_bin]
          path_change = "#{opts[:xtrabackup_bin]}:#{path_change}" if opts[:xtrabackup_bin]
          env["PATH"] = path_change + path_variable unless path_change == ""

          if opts[:dbd_mysql_lib]
            perl5lib_variable = (ENV["PERL5LIB"] && ENV["PERL5LIB"].dup) || ""
            perl5lib_variable = "#{opts[:dbd_mysql_lib]}:#{perl5lib_variable}"
            env["PERL5LIB"] = perl5lib_variable
          end
          env
        end

        def with_env(env = {})
          old_env = ENV.to_hash
          ENV.replace(old_env.merge(env))
          yield
        ensure
          ENV.replace(old_env)
        end

        def backup_mysql_server(dest_folder_name, type, mysql_config, mysqld_properties, dump_path, opts)
          raise ArgumentError, "Missing options." unless type && mysql_config &&
                                                         mysqld_properties && dump_path
          raise ArgumentError, "Unknown backup type" unless ["full", "incremental"].include? type
          make_logger
          host, user, password, port = %w{host user pass port}.map { |opt| mysql_config[opt] }

          env = prepare_env(opts)
          cmd = "innobackupex --host=#{host} --port=#{port} --user=#{user} --password=#{password} "

          defaults_file = "#{dump_path}/my.cnf"
          output_file = "#{dump_path}/backup_output"
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
          res = nil
          with_env(env) { res = VCAP::Services::Base::CMDHandle.execute(cmd, nil, on_err) }

          raise "Failed to execute backup command to #{host}" unless res
          output = File.read(output_file)
          backup_folder = output.match(/Backup created in directory '(.+)'/)[1]
          last_lsn = output.match(/The latest check point \(for incremental\): '(\d+)'/)[1]
          raise "Can't get necessary data from backup output" unless backup_folder && last_lsn
          dest_folder = File.join(File.dirname(backup_folder), dest_folder_name)
          FileUtils.mv(backup_folder, dest_folder)
          {:files => [dest_folder], :last_lsn => last_lsn }
        rescue => e
          @logger.error("Error backup server on host #{host}: #{fmt_error(e)}")
          nil
        ensure
          FileUtils.rm_rf(defaults_file, :secure => true) if defaults_file
          FileUtils.rm_rf(output_file, :secure => true) if output_file
        end

        def restore_mysql_server(dest_folder, backup_folders, opts)
          raise "Invalid backup folders" if backup_folders.empty?
          FileUtils.mkdir_p(dest_folder)

          output_files = backup_folders.map { |f| "#{f}.output" }
          env = prepare_env(opts)
          base_folder = backup_folders.shift
          base_cmd = "innobackupex --apply-log --redo-only #{base_folder}"
          base_output = output_files.shift
          last_apply_output = "#{base_output}.apply_redo"
          cmds = []
          cmds << "#{base_cmd} > #{base_output} 2>&1"
          backup_folders.size.times do |i|
            cmds << "#{base_cmd} --incremental-dir=#{backup_folders[i]} > #{output_files[i]} 2>&1"
          end
          cmds << "innobackupex --apply-log #{base_folder} > #{last_apply_output} 2>&1"
          cmds << "rm #{base_folder}/*.cnf"
          cmds << "cp -r #{base_folder}/* #{dest_folder}"
          # mod will be changed again in warden
          cmds << "chmod -R 777 #{dest_folder}/*"

          output_files.unshift(base_output)
          output_files.unshift(last_apply_output)

          on_err = Proc.new do |command, code, msg|
            raise "CMD '#{command}' exit with code: #{code}. Message: #{msg}"
          end
          res = true
          with_env(env) do
            cmds.each do |cmd|
              @logger.info("Run command: #{cmd}")
              result = VCAP::Services::Base::CMDHandle.execute(cmd, nil, on_err)
              return nil unless result
            end
          end
          output_files.each do |f|
            output = File.read(f)
            unless output =~ /innobackupex: completed OK!/
              @logger.error("Innobackupex cannot apply log for #{File.basename(f, '.*')}")
              return nil
            end
          end
          res
        rescue => e
          @logger.error("Error restore server to #{dest_folder}: #{fmt_error(e)}")
          nil
        ensure
          output_files.each { |f| FileUtils.rm_rf(f, :secure => true) } if output_files
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
            thread_id, user, db = proc["Id"], proc["User"], proc["db"]
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
