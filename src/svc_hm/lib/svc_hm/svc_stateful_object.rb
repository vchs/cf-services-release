module ServicesHealthManager
  module StatefulObject
    def running?
      state == 'RUNNING'
    end

    def down?
      state == 'DOWN'
    end
  end
end

