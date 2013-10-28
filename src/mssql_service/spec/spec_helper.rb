# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

def require_dir(dir_pattern)
  Dir.glob(File.expand_path(dir_pattern, File.dirname(__FILE__))) do |filename|
    require filename
  end
end

require_dir '../vendor/integration-test-support/support/**/*.rb'
require_dir 'support/**/*.rb'

tmp_dir = File.expand_path('../tmp', File.dirname(__FILE__))
FileUtils.mkdir_p(tmp_dir)
IntegrationExampleGroup.tmp_dir = tmp_dir

RSpec.configure do |config|
  config.include IntegrationExampleGroup, type: :integration, :example_group => {:file_path => /\/integration\//}
end

require "rubygems"
require "rspec"
require "bundler/setup"
