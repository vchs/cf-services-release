$LOAD_PATH.unshift File.join(File.dirname(__FILE__))
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require "rubygems"
require "bundler/setup"
require "rspec"
require "backup_manager"
require "yaml"
require "cf_message_bus/mock_message_bus"

def get_local_config
  config_file = File.join(File.dirname(__FILE__), "..", "config", "backup_manager.yml")
  YAML.load_file(config_file)
end

RSpec.configure do |config|
  config.include(BackupManager::Common)
end
