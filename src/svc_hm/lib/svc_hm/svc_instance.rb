require 'set'
require 'svc_hm/svc_stateful_object'
require 'svc_hm/common'

module ServicesHealthManager
  #this class provides info about every single entity running on a specific node
  class Peer
    include StatefulObject
    include Common

    TIMEOUT_PEER_LOST = 60

    attr_reader  :state, :last_heartbeat_time
    def initialize(option)
      @option = option
      @state = 'DOWN'
    end

    # Set down! usually when timeout threshold reached
    def down!
      @state = 'DOWN'
    end

    def receive_heartbeat(state)
      @last_heartbeat_time = now
      res = 0
      if state.eql?('fail')
        new_state = 'DOWN'
        res = -1 if @state.eql?('RUNNING')
      else
        new_state = 'RUNNING'
        res = 1 unless @state.eql?('RUNNING')
      end
      @state = new_state

      res
    end

    def alive?
      running? && has_recent_heartbeat?
    end

    def has_recent_heartbeat?
      !@last_heartbeat_time.nil? && !timestamp_older_than?(@last_heartbeat_time, @option[:timeout] || TIMEOUT_PEER_LOST)
    end

  end

  #this class provides answers about Instance's State
  class Instance
    include Common

    attr_reader :id, :state, :desired_topology, :actual_topology
    attr_accessor :desired_state_update_required

    def initialize(id, option)
      @id = id.to_s
      @desired_topology = {}
      @actual_topology = {}
      @option = option

      reset_tm

      # start out as stale until desired state is set
      @desired_state_update_required = true
      @desired_state_update_timestamp = now

    end

    def ripe_for_gc?
      timestamp_older_than?(@desired_state_update_timestamp, @option[:instance_gc_grace_period])
    end

    def set_desired_state(desired_instance)
      logger.debug("#set_desired_state", { desired_instance: desired_instance })

      %w[state topology updated_at].each do |k|
        unless desired_instance[k]
          raise ArgumentError, "Value #{k} is required, missing from #{desired_instance}"
        end
      end

      @desired_topology = desired_instance['topology']
      @state = desired_instance['state']
      @last_updated = parse_utc(desired_instance['updated_at'])

      @desired_state_update_required = false
      @desired_state_update_timestamp = now
    end

    def to_json(*a)
      encode_json(self.instance_variables.inject({}) do |h, v|
        h[v[1..-1]] = self.instance_variable_get(v); h
      end)
    end

    def reset_tm
      @reset_timestamp = now
    end

    def reset_recently?
      timestamp_fresher_than?(@reset_timestamp, interval(:instance_lost) || 0)
    end

    def desired_state_update_required?
      @desired_state_update_required
    end

    def process_heartbeat(node_info, state)
      logger.info "svc_hm.instance.process_heartbeat: Start MysqlFailOver To be Implemented"

      node_type = node_info[:node_type]
      delta = get_peer(node_info[:node_id]).receive_heartbeat(state[:health])
      #Hack here, just update the info to Gateway
      if delta > 0
        return "svc.health.ok.#{node_type}", { :instance => @id, :heartbeat_time => now }
      elsif delta < 0
        unhealthy_instance = {}
        unhealthy_instance[:instance_id] = @id
        unhealthy_instance[:heartbeat_time] = now
        unhealthy_instance[:actual_states] = { :node_id => node_info[:node_id],
                                                 :health => state[:health]}
        return "svc.health.remedy.#{node_type}", unhealthy_instance
      end
      return nil, nil
    end

    def get_peer(node_id)
      @actual_topology[node_id] ||= Peer.new(@option)
    end

    private

    def desired_state_update_overdue?
      timestamp_older_than?(@desired_state_update_timestamp, interval(:desired_state_lost))
    end

  end
end
