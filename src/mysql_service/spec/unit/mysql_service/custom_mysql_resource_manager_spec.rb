require 'spec_helper'
require 'mysql_service/custom_mysql_resource_manager'

describe VCAP::Services::Mysql::CustomMysqlResourceManager do

  [:create_backup, :delete_backup].each do |method|
    describe method do
      let(:provisioner) { double("provisioner") }
      subject do
        provisioner.stub(:node_nats)
        opts = { :provisioner => provisioner }
        VCAP::Services::Mysql::CustomMysqlResourceManager.new(opts)
      end

      it "should use provisioner to #{method}" do
        args = {"service_id" => "1",
                "backup_id"  => "2",
                "update_url" => "http://test.com"}
        opts = {:type => "full", :trigger_by => "user", :properties => args}
        blk = lambda { |x| }
        provisioner.should_receive(:user_triggered_options).and_return(opts)
        provisioner.should_receive(method).with("1", "2", opts, &blk)

        subject.send(method, nil, args, blk)
      end
    end
  end
end
