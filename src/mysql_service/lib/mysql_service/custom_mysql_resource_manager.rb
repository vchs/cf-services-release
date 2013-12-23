require 'base/custom_resource_manager'
require 'vcap_services_messages/service_message'

class VCAP::Services::Mysql::CustomMysqlResourceManager < VCAP::Services::CustomResourceManager
  include VCAP::Services::Internal


  def update_credential(service_id, args, blk)
    resp = PerformOperationResponse.new({
              :result     => 1,
              :code       => "",
              :properties => {},
              :body       => {}
          })

    begin
      required_options(args, :credentials)
      @provisioner.update_credentials(service_id, args) do |msg|
        if msg["success"]
          resp.result = 0
          resp.code   = "Credential successfully updated"
        else
          resp.result = 1
          resp.code   = "Failed to update credential"
        end
        blk.call(resp.encode)
      end
    rescue => e
      resp.result = 1
      resp.code   = "Failed to update credential"
      @logger.warn("Exception at update_credential: #{e}")
      @logger.warn(e)
      blk.call(resp.encode)
    end
  end

  def create_backup(backup_id, args, blk)
    resp = PerformOperationResponse.new({
      :result     => 1,
      :code       => "",
      :properties => args,
      :body       => {}
    })

    begin
      required_options(args, :service_id, :backup_id, :update_url)
      service_id = args["service_id"]
      backup_id  = args["backup_id"]
      opts = @provisioner.user_triggered_options(args)
      @provisioner.create_backup(service_id, backup_id, opts) do |msg|
        if msg["success"]
          resp.result = 0
          resp.code   = "Backup job successfully triggerred"
        else
          resp.result = 1
          resp.code   = "Creating backup job failed"
        end
        blk.call(resp.encode)
      end
    rescue => e
      resp.result = 1
      resp.code   = "Creating backup job failed"
      @logger.warn("Exception at create_backup: #{e}")
      @logger.warn(e)
      blk.call(resp.encode)
    end
  end
end
