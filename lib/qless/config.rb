# Encoding: utf-8

require 'json'

module Qless
  # A configuration class associated with a qless client
  class Config
    def initialize(client)
      @client = client
    end

    def [](key)
      @client.call('config.get', key)
    end

    def []=(key, value)
      @client.call('config.set', key, value)
    end

    # Get the specified `qless` configuration option, or if
    # none is provided, get the complete current configuration
    def all
      JSON.parse(@client.call('config.get'))
    end

    # Restore this option to the default (remove this option)
    def clear(option)
      @client.call('config.unset', option)
    end
  end
end
