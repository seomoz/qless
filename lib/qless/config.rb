require "qless/lua"
require "redis"
require "json"

module Qless
  # A configuration class associated with a qless client
  class Config
    def initialize(client)
      @client = client
    end
    
    def [](key)
      @client._getconfig.call([], [key])
    end
    
    def []=(key, value)
      @client._setconfig.call([], [key, value])
    end
    
    # Get the specified `qless` configuration option, or if
    # none is provided, get the complete current configuration
    def all
      return JSON.parse(@client._getconfig.call([], []))
    end
    
    # Restore this option to the default (remove this option)
    def clear(option)
      @client._setconfig.call([], [option])
    end
  end
end