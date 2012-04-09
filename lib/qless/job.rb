require "qless/lua"
require "redis"
require "json"

module Qless
  class Job
    attr_reader :id, :expires, :state, :queue, :history, :worker, :retries, :remaining, :failure
    attr_accessor :data, :priority, :tags
    
    def initialize(client, atts)
      @client    = client
      @id        = atts.fetch('id')
      @data      = atts.fetch('data')
      @priority  = atts.fetch('priority').to_i
      @tags      = atts.fetch('tags')
      @worker    = atts.fetch('worker')
      @expires   = atts.fetch('expires').to_i
      @state     = atts.fetch('state')
      @queue     = atts.fetch('queue')
      @retries   = atts.fetch('retries').to_i
      @remaining = atts.fetch('remaining').to_i
      @failure   = atts.fetch('failure', {})
      @history   = atts.fetch('history', [])
      # This is a silly side-effect of Lua doing JSON parsing
      if @tags == {}
        @tags = []
      end
    end
    
    def [](key)
      @data[key]
    end
    
    def []=(key, val)
      @data[key] = val
    end
    
    def to_s
      inspect
    end
    
    def inspect
      "< Qless::Job #{@id} >"
    end
    
    def ttl
      @expires - Time.now.to_i
    end
    
    # Move this from it's current queue into another
    def move(queue)
      @client._put.call([queue], [
        @id, JSON.generate(@data), Time.now.to_i
      ])
    end
    
    # Fail a job
    def fail(t, message)
      @client._fail.call([], [
        @id,
        @worker,
        t, message,
        Time.now.to_i,
        JSON.generate(@data)]) || false
    end
    
    # Heartbeat a job
    def heartbeat()
      @client._heartbeat.call([], [
        @id,
        @worker,
        Time.now.to_i,
        JSON.generate(@data)]) || false
    end
    
    # Complete a job
    # Options include
    # => next (String) the next queue
    # => delay (int) how long to delay it in the next queue
    def complete(options={})
      if options[:next].nil?
        response = @client._complete.call([], [
          @id, @worker, @queue, Time.now.to_i, JSON.generate(@data)])
      else
        response = @client._complete.call([], [
          @id, @worker, @queue, Time.now.to_i, JSON.generate(@data),
          options[:next], (options[:delay] || 0)])
      end
      response.nil? ? false : response
    end
    
    def cancel
      @client._cancel.call([], [@id])
    end
    
    def track(*tags)
      @client._track.call([], ['track', @id, Time.now.to_i] + tags)
    end
    
    def untrack
      @client._track.call([], ['untrack', @id, Time.now.to_i])
    end
  end  
end