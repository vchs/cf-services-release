require 'base/custom_resource_manager'
require 'vcap_services_messages/service_message'

class VCAP::Services::Mysql::CustomMysqlResourceManager < VCAP::Services::CustomResourceManager
  include VCAP::Services::Internal

  def update_credentials(service_id, args, blk)
    resp = PerformOperationResponse.new({
              :result     => 1,
              :code       => "",
              :properties => {},
              :body       => {}
          })

    begin
      required_options(args, :credentials)
      args['credentials'] = JSON.parse(args['credentials']) if args['credentials'].is_a?(String)
      password_len = args['credentials']['password'].length rescue 0
      raise 'No password is given in credentials parameter' unless password_len > 0
      @provisioner.update_credentials(service_id, args) do |msg|
        if msg["success"]
          resp.result = 0
          resp.code   = "Credentials successfully updated"
        else
          resp.result = 1
          resp.code   = "Failed to update credentials"
        end
        blk.call(resp.encode)
      end
    rescue Yajl::ParseError => e
      resp.result = 1
      resp.code   = "Invalid format of 'credentials'"
      @logger.warn("Exception at update_credentials: #{e}")
      @logger.warn(e)
      blk.call(resp.encode)

    rescue => e
      resp.result = 1
      resp.code   = "Failed to update credentials"
      @logger.warn("Exception at update_credentials: #{e}")
      @logger.warn(e)
      blk.call(resp.encode)
    end
  end

  def create_backup(backup_id, args, blk)
    handle_backup([:service_id, :backup_id, :update_url],
                  "CreateBackupJob successfully triggerred",
                  "CreateBackupJob failed",
                  :create_backup, args, blk)
  end

  def delete_backup(backup_id, args, blk)
    handle_backup([:service_id, :backup_id],
                  "DeleteBackupJob successfully triggerred",
                  "DeleteBackupJob failed",
                  :delete_backup, args, blk)
  end

  def handle_backup(required_fields, success_code, error_code, method_name, args, blk)
    resp = PerformOperationResponse.new({
      :result     => 1,
      :code       => "",
      :properties => args,
      :body       => {}
    })

    begin
      required_options(args, *required_fields)
      service_id = args["service_id"]
      backup_id  = args["backup_id"]
      opts = @provisioner.user_triggered_options(args)
      @provisioner.send(method_name, service_id, backup_id, opts) do |msg|
        if msg["success"]
          resp.result = 0
          resp.code   = success_code
        else
          resp.result = 1
          resp.code   = error_code
        end
        blk.call(resp.encode)
      end
    rescue => e
      resp.result = 1
      resp.code   = error_code
      @logger.warn("Exception at #{method_name}: #{e}")
      @logger.warn(e)
      blk.call(resp.encode)
    end
  end
end
