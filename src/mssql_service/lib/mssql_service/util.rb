# Copyright (c) 2013-2015 VMware, Inc.
require "pp"
require "securerandom"
require "uri"

module VCAP
  module Services
    module MSSQL
      module Util
        PASSWORD_LENGTH = 9
        DBNAME_LENGTH = 9

        def password_length
          PASSWORD_LENGTH
        end

        def dbname_length
          DBNAME_LENGTH
        end

        def generate_credential(length=9)
          SecureRandom.uuid.to_s.gsub(/-/, '')[0, length]
        end

        def generate_service_id
          SecureRandom.uuid.to_s.gsub("-", "")[0, dbname_length]
        end

        def log(*args) #:nodoc:
          args.unshift(Time.now)
          PP::pp(args.compact, $stdout, 120)
        end

        def debug(*args) #:nodoc:
          log(*args)
        end

        def trace(*args) #:nodoc:
          log(*args)
        end
      end
    end
  end
end