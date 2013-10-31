class MssqlRunner < ComponentRunner
  def start(opts=nil)
    Dir.chdir(File.expand_path("../..", File.dirname(__FILE__))) do
      Bundler.with_clean_env do
        sh "bundle install >> #{tmp_dir}/log/bundle.out"
        config = MssqlConfig.new(self, opts)
        add_pid Process.spawn(
          {"AUTHORIZATION_TOKEN" => ccng_auth_token},
          "bundle exec bin/mssql_gateway -c #{config.gateway_file_location}",
          log_options(:mssql_gateway)
        )
        wait_for_tcp_ready('Mssql Gateway', config.gateway_config_hash.fetch('port'))

        begin
          service_name = config.gateway_config_hash["service"]["name"]
          service_provider = config.gateway_config_hash["service"]["provider"] || "core"
          create_service_auth_token(
            service_name,
            'mssql-token',
            service_provider
          )
        rescue CcngClient::UnsuccessfulResponse
          # Failed to add auth token, likely due to duplicate
        end

        add_pid Process.spawn(
          "bundle exec bin/mssql_node -c #{config.node_file_location}",
          log_options(:mssql_node)
        )

        sleep 5
      end
    end
  end

  class MssqlConfig
    attr_reader :runner

    def initialize(runner, opts)
      @runner = runner
      write_custom_config(opts)
    end

    def gateway_file_location
      "#{runner.tmp_dir}/config/mssql_gateway.yml"
    end

    def node_file_location
      "#{runner.tmp_dir}/config/mssql_node.yml"
    end

    def gateway_config_hash
      YAML.load_file(gateway_file_location)
    end

    def node_config_hash
      YAML.load_file(node_file_location)
    end

    private

    def base_gateway_config_hash
      YAML.load_file(runner.asset("mssql_gateway.yml"))
    end

    def base_node_config_hash
      YAML.load_file(runner.asset("mssql_node.yml"))
    end

    def write_custom_config(opts)
      FileUtils.mkdir_p("#{runner.tmp_dir}/config")
      gateway_config_hash = base_gateway_config_hash
      node_config_hash = base_node_config_hash
      if opts
        gateway_config_hash['service']['name'] = opts[:service_name] if opts.has_key?(:service_name)
        gateway_config_hash['service']['provider'] = opts[:service_provider] if opts.has_key?(:service_provider)
        gateway_config_hash['service']['blurb'] = opts[:service_blurb] if opts.has_key?(:service_blurb)
        if opts.has_key?(:plan_name)
          gateway_config_hash['service']['plans'] = {opts.fetch(:plan_name) => base_gateway_config_hash.fetch('service').fetch('plans').values.first}
          node_config_hash['plan'] = opts.fetch(:plan_name)
        end
        # ensure that the gateway has a key for the service
        gateway_config_hash['service_auth_tokens'] = {
          "#{gateway_config_hash['service']['name']}_#{gateway_config_hash['service']['provider'] || 'core'}" => 'mssql-token'
        }
      end

      File.write(gateway_file_location, YAML.dump(gateway_config_hash))
      File.write(node_file_location, YAML.dump(node_config_hash))
    end
  end

end
