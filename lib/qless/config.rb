require "qless/lua"
require "redis"
require "json"

module Qless
  # A configuration class associated with a qless client
  class Config
    def initialize(redis)
      @redis = redis
      @get   = Lua.new('getconfig', redis)
      @set   = Lua.new('setconfig', redis)
    end
    
    def [](key)
      @get.call([], [key])
    end
    
    def []=(key, value)
      @set.call([], [key, value])
    end
    
    # Get the specified `qless` configuration option, or if
    # none is provided, get the complete current configuration
    def all
      # Taken from https://github.com/ezmobius/redis-rb/blob/master/lib/redis.rb
      hash = Hash.new
      @get.call([], []).each_slice(2) do |field, value|
        hash[field] = value
      end
      hash
    end
    
    # Restore this option to the default (remove this option)
    def clear(option)
      @set.call([], [option])
    end
  end
end