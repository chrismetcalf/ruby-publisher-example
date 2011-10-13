require 'singleton'
require 'yaml'

['utils'].each do |lib|
  require File.join(File.dirname(__FILE__), "/#{lib}")
end

class ConfigCache
  include Singleton

  def initialize
    @config = YAML.load_file(File.join(File.dirname(__FILE__), "../config/config.yml"))
    @config.recursively_symbolize_keys!
  end

  def [] key
    @config[key]
  end
end
