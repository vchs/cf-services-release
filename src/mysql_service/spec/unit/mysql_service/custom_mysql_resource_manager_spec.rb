require 'spec_helper'
require 'mysql_service/custom_mysql_resource_manager'

describe VCAP::Services::Mysql::CustomMysqlResourceManager do
  before do
    VCAP::Services::Mysql::CustomMysqlResourceManager.any_instance.stub(:initialize)
    VCAP::Services::Mysql::Provisioner.any_instance.stub(:initialize)
  end

  describe "#create_backup" do
    let(:provisioner) { VCAP::Services::Mysql::Provisioner.new }

    it "should use provisioner to create backup" do
      args = {"service_id" => "1",
              "backup_id"  => "2",
              "update_url" => "http://test.com"}
      opts = {:type => "full", :trigger_by => "user", :properties => args}
      blk = lambda { |x| }
      provisioner.should_receive(:create_backup).with("1", "2", opts, blk).
        and_return({"success" => true})

      subject.instance_variable_set(:@provisioner, provisioner)
      subject.create_backup(nil, args, blk)
    end
  end
end
