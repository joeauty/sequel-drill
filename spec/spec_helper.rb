require 'rubygems'
require "sequel"
require "json"
require "net/http"
require "webhdfs"
require "pry"

begin
  require 'spec_config'
rescue LoadError
  $stderr.puts "Please create a file spec/spec_config.rb with a database connection string."
  raise
end

RSpec.configure do |config|
  config.run_all_when_everything_filtered = true
  config.filter_run focus: true
end
