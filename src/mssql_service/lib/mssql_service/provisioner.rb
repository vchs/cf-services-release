# Copyright (c) 2009-2011 VMware, Inc.

$LOAD_PATH.unshift File.join(File.dirname __FILE__)
require 'common'

class VCAP::Services::Mssql::Provisioner < VCAP::Services::Base::Provisioner
  include VCAP::Services::Mssql::Common

  def initialize(opts)
    super(opts)
  end

  #FIXME: This is a stub function since we have not achieved all functionalities.
  def provision_service(request, prov_handle=nil, &blk)
    @logger.debug("[#{service_description}] Attempting to provision instance (request=#{request.extract})")

    configuration = "free"
    service_id = "517D31FA-5A48-43E2-B2E4-4AA58F82C6C5"
    credential = {
        username: "test",
        password: "test",
        connstring: "mssql://localhost:1234"
    }

    @logger.debug("Successfully proovision response from HM for #{service_id}")

    svc = { configuration: configuration,
            service_id:    service_id,
            credentials:   credential }
    blk.call(success(svc))
  end

  #NOTE: base proivision_service method rely on the foillowing functions
  def generate_service_id
    #TODO
  end

  def generate_recipes(service_id, plan_config, version, best_nodes)
    #TODO
  end
end
