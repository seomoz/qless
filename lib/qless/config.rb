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
      return get(key)
    end
    
    def []=(key, value)
      return set(key, value)
    end
    
    # Get the specified `qless` configuration option, or if
    # none is provided, get the complete current configuration
    def get(option=nil)
      if option.nil?
        # Taken from https://github.com/ezmobius/redis-rb/blob/master/lib/redis.rb
        hash = Hash.new
        @get.call([], []).each_slice(2) do |field, value|
          hash[field] = value
        end
        hash
      else
        return JSON.parse(@get.call([], [option]))
      end
    end
    
    # Set the provided option to the provided value. In the absence
    # of a value, it will unset that option, restoring it to the
    # default
    def set(option, value=None)
      if not value.nil?
        return @set.call([], [option, value])
      else
        return @set.call([], [option])
      end
    end
  end
end