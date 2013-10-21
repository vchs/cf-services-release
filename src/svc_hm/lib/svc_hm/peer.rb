require 'svc_hm/common'
require 'state_machine'

module ServicesHealthManager
  #this class provides info about every single entity running on a specific node
  class Peer
    include Common

    attr_reader :last_heartbeat_time

    state_machine :initial => :unknown do
      event :get_good_health do
        transition [:unknown, :running] => :running
      end

      event :get_bad_health do
        transition [:unknown, :running] => :stopped
      end

      event :lose_heartbeat do
        transition :running => :unknown
      end

      event :health_timeout do
        transition :unknown => :timeout
      end

      event :unregister do
        transition [:stopped, :timeout] => :terminated
      end
    end

    def initialize(option)
      @option = option
      super()
    end

    def receive_heartbeat(state)
      @last_heartbeat_time = now
      res = 0
      if state.eql?('fail')
        res = -1 if @state.eql?('running')
        get_bad_health
      else
        res = 1 unless @state.eql?('running')
        get_good_health
      end

      res
    end

    def alive?
      running? && has_recent_heartbeat?
    end

    def has_recent_heartbeat?
      @last_heartbeat_time &&
        !timestamp_older_than?(@last_heartbeat_time,
                               @option[:timeout] || Common::TIMEOUT_PEER_LOST)
    end
  end
end
