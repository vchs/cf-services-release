# Copyright (c) 2009-2011 VMware, Inc.

module VCAP
  module Services
    module Mssql
      class Node < VCAP::Services::Base::Node
        class ProvisionedService
        end
      end
    end
  end
end

require "mssql_service/common"

class VCAP::Services::Mssql::Node
  include VCAP::Services::Mssql::Common

  def initialize(opts)
    super(opts)
  end

  #NOTE: a fake function
  def provision(plan, credential=nil, version=nil)
    {
        "name" => "test",
        "hostname" => "localhost",
        "host" => "localhost",
        "port" => "1433",
        "user" => "test",
        "username" => "test",
        "password" => "123456",
        "uri" => "mssql://test:123456@localhost:1433"
    }
  end

  #NOTE: a fake function
  def announcement
    {
      :available_capacity => 180,
      :max_capacity => 200,
      :capacity_unit => 1,
      :host => "127.0.0.1"
    }
  end
end
