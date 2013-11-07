require 'spec_helper'
require 'mysql_service/node'
require 'mysql_service/util'
require 'mysql_service/job'

module VCAP::Services::Mysql::Backup
  describe CreateBackupJob, components: [:nats], hook: :all do
    include VCAP::Services::Base::AsyncJob

    before :all do
      `which redis-server`
      pending "Redis not installed" unless $?.success?
      start_redis

      config = Yajl::Parser.parse(ENV["WORKER_CONFIG"])
      VCAP::Services::Base::AsyncJob::Config.redis_config = config["resque"]
      VCAP::Services::Base::AsyncJob::Config.logger = getLogger

      Resque.inline = true

      Fog.mock!
      @connection = Fog::Storage.new({
        :aws_access_key_id      => 'fake_access_key_id',
        :aws_secret_access_key  => 'fake_secret_access_key',
        :provider               => 'AWS'
      })
      StorageClient.instance_variable_set(:@storage_connection, @connection)

      @opts = getNodeTestConfig
      default_version = @opts[:default_version]
      @use_warden = @opts[:use_warden]
      EM.run do
        @node = VCAP::Services::Mysql::Node.new(@opts)
        EM.add_timer(1) do
          @db = @node.provision(@opts[:plan], nil, default_version)
          @db_instance = @node.mysqlProvisionedService.get(@db["name"])
          EM.stop
        end
      end if @use_warden
    end

    after :all do
      EM.run do
        @node.unprovision(@db["name"], [])
        EM.stop
      end if @use_warden
      stop_redis
    end

    it "should be able to create full & incremental backup" do
      service_id = @db_instance.name
      ["full", "incremental"].each do |type|
        job_id = CreateBackupJob.create(:service_id => service_id,
                                        :node_id    => @opts[:node_id],
                                        :metadata   => {:type => type}
                                       )

        instance_backup_info = DBClient.get_instance_backup_info(service_id)
        instance_backup_info.should_not be_nil
        instance_backup_info.values_at(:last_lsn, :last_backup).should_not include(nil)

        job_status = get_job(job_id)
        backup_id = job_status[:result]["backup_id"]
        single_backup_info = DBClient.get_single_backup_info(service_id, backup_id)
        single_backup_info.should_not be_nil
        single_backup_info.values_at(:backup_id, :type, :date, :manifest).should_not include(nil)

        StorageClient.get_file("mysql", service_id, backup_id).should_not be_nil
      end
    end
  end
end
