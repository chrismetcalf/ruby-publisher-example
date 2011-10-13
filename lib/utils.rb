class Array
  def super_compact
    self.compact.delete_if { |i| i.is_a?(String) && i.strip.cleanup.empty? }
  end

  def recursively_symbolize_keys!
    self.each do |item|
      if item.is_a? Hash
        item.recursively_symbolize_keys!
      elsif item.is_a? Array
        item.recursively_symbolize_keys!
      end
    end
  end
end

class Hash
  def symbolize_keys!
    keys.each do |key|
      self[(key.to_sym rescue key) || key] = delete(key)
    end
    self
  end

  def recursively_symbolize_keys!
    self.symbolize_keys!
    self.values.each do |v|
      if v.is_a? Hash
        v.recursively_symbolize_keys!
      elsif v.is_a? Array
        v.recursively_symbolize_keys!
      end
    end
    self
  end
end

class String
  def cleanup
    return self.strip.gsub(/^null$/i, "")
  end

  def underscore
    self.gsub(/([A-Z]+)([A-Z][a-z])/,'\1_\2').
      gsub(/([a-z\d])([A-Z])/,'\1_\2').
      gsub(/[^A-z_0-9]+/, '_').
      downcase
  end
end

class Logger
  def self.debug(message)
    puts "[#{Time.now}] (debug) #{message}"
  end

  def self.info(message)
    puts "[#{Time.now}] (info) #{message}"
  end

  def self.warn(message)
    puts "[#{Time.now}] (warn) #{message}"
  end

  def self.critical(message)
    puts "[#{Time.now}] (critical) #{message}"
  end
end

