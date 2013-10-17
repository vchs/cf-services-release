$LOAD_PATH.unshift File.join(File.dirname(__FILE__))
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'bundler/setup'
require 'rspec'
require 'svc_hm'
require 'cf_message_bus/mock_message_bus'

def get_local_config
  config_file = File.join(File.dirname(__FILE__), '..', 'config', 'svc_hm.yml')
  YAML.load_file(config_file)
end

RSpec.configure do |config|
  config.include(ServicesHealthManager::Common)
end
