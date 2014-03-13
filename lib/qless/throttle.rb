# Encoding: utf-8

require 'redis'
require 'json'

module Qless
  class Throttle
    attr_reader :name, :client

    def initialize(name, client)
      @name = name
      @client = client
    end

    def delete
      @client.call('throttle.delete', @name)
    end

    def id
      @name
    end

    def locks
      @client.call('throttle.locks', @name)
    end

    def maximum
      throttle_attrs['maximum'].to_i
    end

    def maximum=(max)
      @client.call('throttle.set', @name, max)
    end

    def pending
      @client.call('throttle.pending', @name)
    end

    private
    def throttle_attrs
      throttle_json = @client.call('throttle.get', @name)
      throttle_json ? JSON.parse(throttle_json) : {}
    end

  end
end
