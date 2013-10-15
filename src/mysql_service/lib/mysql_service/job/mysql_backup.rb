$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..')
require "util"
require "mysql_error"
require "datamapper_l"
require "node"

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
    include Common

    def perform
      begin
        required_options :service_id
        @name = options["service_id"]
        @metadata = VCAP.symbolize_keys(options["metadata"])
        @type = @metadata[:type]
        @logger.info("Launch job: #{self.class} for #{name} with metadata: #{@metadata}")

        @backup_id = UUIDTools::UUID.random_create.to_s
        @backup_files = []
        lock = create_lock
        lock.lock do
          backup = execute
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
          @logger.info("Package backup file #{File.join(dump_path, package_file)}")
          backup[:single_backup][:date] = fmt_time

          StorageClient.store_file(@config["service_name"], name, backup_id, package_file_path)
          DBClient.execute_as_transaction do
            DBClient.set_instance_backup_info(name, backup[:instance_info])
            DBClient.set_single_backup_info(name, backup_id, backup[:single_backup])
          end

          @backup_files << package_file_path

          completed(Yajl::Encoder.encode(filter_keys(backup[:single_backup])))
          @logger.info("Complete job: #{self.class} for #{name}")
        end
      rescue => e
        handle_error(e)
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

      result = backup_mysql_server(@type, mysql_conf, mysqld_properties, dump_path, backup_conf)
      backup = {
        :files => result[:files],
        :instance_info => {
          :last_lsn => result[:last_lsn],
          :last_backup => backup_id
        },
        :single_backup => {
          :backup_id => backup_id,
          :type => @type,
          :manifest => {
            :version => 1,
            :service_version => srv.version
          }
        },
      }
      backup[:single_backup][:previous_bakcup] = backup_conf["last_bakcup"] if @type == "incremental"
      backup[:instance_info][:node_id] = @config["node_id"] if @type == "full"

      backup
    end
  end
end
