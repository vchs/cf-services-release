# Copyright (c) 2013-2015 VMware, Inc.

module VCAP::Services::MSSQL
  module Task
    def self.included(base)
      base.extend(ClassMethods)
    end

    module ClassMethods
      def create(options = {})
        MessageQueue.enqueue(self, options)
      end

      def queue_lookup_key
        :node_id
      end

      def select_queue(*args)
        queue = nil
        args.each do |arg|
          queue = arg[queue_lookup_key] if arg.is_a?(Hash) && arg.has_key?(queue_lookup_key)
        end
        queue
      end
    end
  end

  class BackupTask
    include VCAP::Services::MSSQL::Task
  end

  class DeleteBackupTask
    include VCAP::Services::MSSQL::Task
  end

  class RestoreTask
    include VCAP::Services::MSSQL::Task
  end

  module Messages
    class BackupTaskResponse < ServiceMessage
      required :id
      required :result,      String
      required :properties,  Hash
      optional :error
    end
  end
end