module BackupManager
  module Common

    CHAN_GW_HANDLES = "gw.handles".freeze
    CHAN_BM_HANDLES = "bm.handles".freeze

    @config = {}
    class << self
      attr_accessor :config
    end

    def logger
      @logger ||= Steno.logger("backup_manager")
    end
  end
end
