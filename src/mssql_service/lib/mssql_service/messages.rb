# Copyright (c) 2013-2015 VMware, Inc.

module VCAP::Services::MSSQL
  module Messages
    class BackupTaskResponse < ServiceMessage
      required :id
      required :result,      String
      required :properties,  Hash
      optional :error
    end
  end
end