# Copyright (c) 2009-2011 VMware, Inc.

$LOAD_PATH.unshift File.join(File.dirname __FILE__)
require 'common'

class VCAP::Services::Mssql::Provisioner < VCAP::Services::Base::Provisioner
  include VCAP::Services::Mssql::Common

  def initialize(opts)
    super(opts)
  end

  #NOTE: base proivision_service method rely on the foillowing functions
  def generate_service_id
    @logger.debug("#{__method__} is invoked ...")

    #TODO
  end

  def generate_recipes(service_id, plan_config, version, best_nodes)
    @logger.debug("#{__method__} is invoked ...")

    #TODO
  end
end
