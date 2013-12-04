# Copyright (c) 2013-2015 VMware, Inc.

module VCAP::Services::MSSQL
  module Job
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

  class BackupJob
    include VCAP::Services::MSSQL::Job
  end
end