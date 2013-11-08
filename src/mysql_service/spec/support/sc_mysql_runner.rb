class ScMysqlRunner < ComponentRunner
  def start_redis
    add_pid Process.spawn "redis-server #{asset "redis.conf"}", log_options(:redis)
    wait_for_tcp_ready("Redis", 5454)
  end

  def start(opts=nil)
    Dir.chdir(File.expand_path("../..", File.dirname(__FILE__))) do
      Bundler.with_clean_env do
        sh "bundle install >> #{tmp_dir}/log/bundle.out"
        config = MysqlConfig.new(self, opts)
        add_pid Process.spawn(
          "bundle exec bin/mysql_gateway -c #{config.gateway_file_location}",
          log_options(:sc_mysql_gateway)
        )
        wait_for_tcp_ready('Mysql Gateway',
                           config.gateway_config_hash.fetch('port'))

        add_pid Process.spawn(
          "bundle exec bin/mysql_node -c #{config.node_file_location}",
          log_options(:sc_mysql_node)
        )
        Dir.chdir File.join(tmp_dir, 'svc_hm') do
          add_pid Process.spawn(
            "bundle exec bin/svc_hm",
            log_options(:svc_hm)
          )
        end
        sleep 5
      end
    end
  end

  def stop
    super
  ensure
    FileUtils.rm_rf('/tmp/mysql_integration_test')
  end

  class MysqlConfig
    attr_reader :runner

    def initialize(runner, opts)
      @runner = runner
      write_custom_config(opts)
    end

    def gateway_file_location
      "#{runner.tmp_dir}/config/sc_mysql_gateway.yml"
    end

    def node_file_location
      "#{runner.tmp_dir}/config/sc_mysql_node.yml"
    end

    def gateway_config_hash
      YAML.load_file(gateway_file_location)
    end

    def node_config_hash
      YAML.load_file(node_file_location)
    end

    private

    def base_gateway_config_hash
      YAML.load_file(runner.asset("sc_mysql_gateway.yml"))
    end

    def base_node_config_hash
      YAML.load_file(runner.asset("sc_mysql_node.yml"))
    end

    def write_custom_config(opts)
      FileUtils.mkdir_p("#{runner.tmp_dir}/config")
      gateway_config_hash = base_gateway_config_hash
      node_config_hash = base_node_config_hash
      File.write(gateway_file_location, YAML.dump(gateway_config_hash))
      File.write(node_file_location, YAML.dump(node_config_hash))
    end
  end
end
