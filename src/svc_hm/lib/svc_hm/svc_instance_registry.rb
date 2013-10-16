require 'svc_hm/svc_instance'
module ServicesHealthManager
  class InstanceRegistry < Hash
    def get(id)
      self[id.to_s] ||= ServicesHealthManager::Instance.new(id)
    end
  end
end


