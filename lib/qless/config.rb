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
      @client._config.call([], ['get', key])
    end
    
    def []=(key, value)
      @client._config.call([], ['set', key, value])
    end
    
    # Get the specified `qless` configuration option, or if
    # none is provided, get the complete current configuration
    def all
      return JSON.parse(@client._config.call([], ['get']))
    end
    
    # Restore this option to the default (remove this option)
    def clear(option)
      @client._config.call([], ['unset', option])
    end
  end
end