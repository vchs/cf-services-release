# Copyright (c) 2009-2011 VMware, Inc.

$LOAD_PATH.unshift File.join(File.dirname __FILE__)

class VCAP::Services::Mssql::Provisioner < VCAP::Services::Base::Provisioner
  def initialize(opts)
  end

  #NOTE: base proivision_service method rely on the foillowing functions
  def generate_service_id
    #TODO
  end

  def generate_recipes(service_id, plan_config, version, best_nodes)
    #TODO
  end
end
