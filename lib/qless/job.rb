require "qless/lua"
require "redis"
require "json"
require "uuid"

module Qless
  class Job
    attr_reader :jid, :expires, :state, :queue, :history, :worker, :retries, :remaining, :failure, :klass, :delay, :tracked
    attr_accessor :data, :priority, :tags
    
    def perform
      klass = @klass.split('::').inject(nil) { |m, el| (m || Kernel).const_get(el) }
    end
    
    def initialize(client, atts)
      @jid       = atts.fetch('jid')
      @client    = client
      @data      = atts.fetch('data')
      @klass     = atts.fetch('klass')
      @priority  = atts.fetch('priority').to_i
      @tags      = atts.fetch('tags')
      @worker    = atts.fetch('worker')
      @expires   = atts.fetch('expires').to_i
      @state     = atts.fetch('state')
      @tracked   = atts.fetch('tracked')
      @queue     = atts.fetch('queue')
      @retries   = atts.fetch('retries').to_i
      @remaining = atts.fetch('remaining').to_i
      @delay     = atts.fetch('delay', 0).to_i
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
      "< Qless::Job #{@jid} >"
    end
    
    def ttl
      @expires - Time.now.to_i
    end
    
    # Move this from it's current queue into another
    def move(queue)
      @client._put.call([queue], [
        @jid, @klass, JSON.generate(@data), Time.now.to_i
      ])
    end
    
    # Fail a job
    def fail(group, message)
      @client._fail.call([], [
        @jid,
        @worker,
        group, message,
        Time.now.to_i,
        JSON.generate(@data)]) || false
    end
    
    # Heartbeat a job
    def heartbeat()
      @client._heartbeat.call([], [
        @jid,
        @worker,
        Time.now.to_i,
        JSON.generate(@data)]) || false
    end
    
    # Complete a job
    # Options include
    # => next (String) the next queue
    # => delay (int) how long to delay it in the next queue
    def complete(nxt=nil, options={})
      if nxt.nil?
        response = @client._complete.call([], [
          @jid, @worker, @queue, Time.now.to_i, JSON.generate(@data)])
      else
        response = @client._complete.call([], [
          @jid, @worker, @queue, Time.now.to_i, JSON.generate(@data),
          nxt, (options[:delay] || 0)])
      end
      response.nil? ? false : response
    end
    
    def cancel
      @client._cancel.call([], [@jid])
    end
    
    def track(*tags)
      @client._track.call([], ['track', @jid, Time.now.to_i] + tags)
    end
    
    def untrack
      @client._track.call([], ['untrack', @jid, Time.now.to_i])
    end
  end  
end