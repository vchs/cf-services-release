$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..')
require "util"
require "mysql_error"
require "datamapper_l"
require_relative "../node"

module VCAP::Services::Mysql::Backup
  include VCAP::Services::Base::AsyncJob::Backup

  module Common
    def init_localdb(database_url)
      DataMapper.setup(:default, database_url)
    end

    def mysql_provisioned_service(use_warden)
      VCAP::Services::Mysql::Node.mysqlProvisionedServiceClass(use_warden)
    end
  end

  class CreateBackupJob < BackupJob
    include VCAP::Services::Mysql::Util
    include VCAP::Services::Mysql::Common
    include Common

    BACKUP_CHANNEL = "create_backup".freeze

    def perform
      begin
        required_options :service_id, :backup_id
        @name = options["service_id"]
        @backup_id = options["backup_id"]
        @metadata = VCAP.symbolize_keys(options["metadata"])
        @type = @metadata[:type]
        @logger.info("Launch job: #{self.class} for #{name} with metadata: #{@metadata}")

        @backup_files = []
        lock = create_lock
        lock.lock do
          backup = execute
          raise "Job #{self.class} failed for #{name}" unless backup
          backup = VCAP.symbolize_keys backup

          backup[:single_backup][:manifest] ||= {}
          backup[:single_backup][:manifest].merge! @metadata
          @logger.info("Results of create backup: #{backup.inspect}")

          dump_path = get_dump_path
          package_file = "#{backup_id}.zip"
          package_file_path = File.join(dump_path, package_file)
          package = VCAP::Services::Base::AsyncJob::Package.new(package_file_path)
          package.manifest = backup[:single_backup][:manifest]
          files = Array(backup[:files])
          raise "No backup file to package." if files.empty?

          files.each do |f|
            @backup_files << f
            package.add_files f
          end
          package.pack
          @backup_files << package_file_path
          @logger.info("Package backup file #{File.join(dump_path, package_file)}")
          File.open(package_file_path) { |f| backup[:single_backup][:size] = f.size }
          backup[:single_backup][:date] = fmt_time

          StorageClient.store_file(service_name, name, backup_id, package_file_path)
          if @metadata[:trigger_by] == "user"
            properties = @metadata[:properties]
            properties["size"] = backup[:single_backup][:size]
            properties["date"] = backup[:single_backup][:date]

            send_msg("#{service_name}.#{BACKUP_CHANNEL}",
                     success_response(backup_id, properties))
          else
            DBClient.execute_as_transaction do
              DBClient.set_instance_backup_info(name, backup[:instance_info])
              DBClient.set_single_backup_info(name, backup_id, backup[:single_backup])
            end
          end

          completed(Yajl::Encoder.encode(filter_keys(backup[:single_backup])))
          @logger.info("Complete job: #{self.class} for #{name}")
        end
      rescue => e
        StorageClient.delete_file(service_name, name, backup_id)
        err_msg = handle_error(e)
        if @metadata[:trigger_by] == "user"
          send_msg("#{service_name}.#{BACKUP_CHANNEL}",
                   failed_response(backup_id, @metadata[:properties], err_msg))
        end
      ensure
        set_status({:complete_time => Time.now.to_s})
        @backup_files.each{|f| FileUtils.rm_rf(f, :secure => true) if File.exists? f} if @backup_files
      end
    end

    def execute
      use_warden = @config["use_warden"] || false
      dump_path = get_dump_path
      FileUtils.mkdir_p(dump_path)

      init_localdb(@config["local_db"])
      srv =  mysql_provisioned_service(use_warden).get(name)
      raise "Can't find service instance:#{name}" unless srv
      mysql_conf = @config["mysql"][srv.version]
      mysql_conf["host"] = srv.ip if use_warden

      mysqld_properties = @config["mysqld"][srv.version]
      mysqld_properties["datadir"] << "/#{name}/data" if use_warden

      backup_conf = VCAP.symbolize_keys(@config["backup"] || {})
      if @type == "incremental"
        instance_backup_info = DBClient.get_instance_backup_info(name)
        raise "Missing previous backup information for incremental backup" unless instance_backup_info
        backup_conf[:last_lsn] = instance_backup_info[:last_lsn]
        backup_conf[:last_backup] = instance_backup_info[:last_backup]
      end

      result = backup_mysql_server(backup_id, @type, mysql_conf, mysqld_properties, dump_path, backup_conf)
      raise "Backup Error" unless result
      backup = {
        :files => result[:files],
        :single_backup => {
          :backup_id => backup_id,
          :type => @type,
          :manifest => {
            :version => 1,
            :service_version => srv.version
          }
        }
      }
      backup[:single_backup][:previous_backup] = backup_conf[:last_backup] if @type == "incremental"

      backup[:instance_info] = {
          :last_lsn => result[:last_lsn],
          :last_backup => backup_id
      } unless @metadata[:trigger_by] == "user"

      backup
    end
  end

  class RestoreBackupJob < BackupJob
    include VCAP::Services::Mysql::Util
    include VCAP::Services::Mysql::Common
    include Common

    BACKUP_CHANNEL = "restore_backup".freeze

    def perform
      begin
        required_options :service_id, :original_service_id, :backup_id
        @name = options["service_id"]
        @original_service_id = options["original_service_id"]
        @backup_id = options["backup_id"]
        @metadata = VCAP.symbolize_keys(options["metadata"])
        @type = @metadata[:type]
        @logger.info("Launch job: #{self.class} for #{name} with metadata: #{@metadata}")

        @backup_files = []
        @data_dir = nil
        response = SimpleResponse.new
        lock = create_lock
        lock.lock do
          result = execute
          @logger.info("Results of restore backup: #{result}")

          response.success = true
          send_msg("#{service_name}.#{BACKUP_CHANNEL}.#{name}", response.encode) do
            FileUtils.rm_rf(File.dirname(@data_dir), :secure => true) if @data_dir
          end

          completed(Yajl::Encoder.encode({:result => :ok}))
          @logger.info("Complete job: #{self.class} for #{name}")
        end
      rescue => e
        # remove the parenet folder which is the instance folder
        FileUtils.rm_rf(File.dirname(@data_dir), :secure => true) if @data_dir
        response.success = false
        response.error = handle_error(e)
        send_msg("#{service_name}.#{BACKUP_CHANNEL}.#{name}", response.encode)
      ensure
        set_status({:complete_time => Time.now.to_s})
        @backup_files.each{|f| FileUtils.rm_rf(f, :secure => true) if File.exists? f} if @backup_files
      end
    end

    def execute
      use_warden = @config["use_warden"] || false
      dump_path = get_dump_path
      backup_ids = [@backup_id]

      unless @metadata[:trigger_by] == "user"
        bid = @backup_id
        loop do
          info = DBClient.get_single_backup_info(@original_service_id, bid)
          bid = info[:previous_backup]
          bid.nil? ? break : backup_ids.unshift(bid)
        end
      end

      backup_ids.each do |id|
        package_file = File.join(dump_path, "#{id}.zip")
        StorageClient.get_file(service_name, @original_service_id, id, package_file)
        @backup_files << package_file
        package = VCAP::Services::Base::AsyncJob::Package.load(package_file)
        package.unpack(dump_path)
        @backup_files << File.join(dump_path, id)
        @logger.debug("Unpack files from #{package_file} of instance #{@original_service_id}")
      end
      backup_folders = backup_ids.map { |id| File.join(dump_path, id) }

      mysqld_properties = @config["mysqld"][@metadata[:properties][:service_version]]
      @data_dir = mysqld_properties["datadir"]
      @data_dir << "/#{name}/data" if use_warden
      backup_conf = VCAP.symbolize_keys(@config["backup"] || {})
      result = restore_mysql_server(@data_dir, backup_folders, backup_conf)
      raise "Failed to execute restore command to #{name}" unless result

      true
    end
  end

end
