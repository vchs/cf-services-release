require 'svc_hm/svc_instance'
module ServicesHealthManager
  class InstanceRegistry < Hash
    def get(id, option)
      self[id.to_s] ||= ServicesHealthManager::Instance.new(id, option)
    end
  end
end


