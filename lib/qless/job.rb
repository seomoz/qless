require "qless/lua"
require "redis"
require "json"

module Qless
  class Job
    attr_reader :id, :expires, :state, :queue, :history, :worker, :retries, :remaining, :failure
    attr_accessor :data, :priority, :tags
    
    def initialize(redis, atts)
      @redis     = redis
      @id        = atts['id']             or throw 'Missing ID'
      @data      = atts['data']           or throw 'Missing user data'
      @priority  = atts['priority'].to_i  or throw 'Missing priority'
      @tags      = atts['tags']           or throw 'Missing tags'
      @worker    = atts['worker']         or throw 'Missing worker'
      @expires   = atts['expires'].to_i   or throw 'Missing expires'
      @state     = atts['state']          or throw 'Missing state'
      @queue     = atts['queue']          or throw 'Missing queue'
      @retries   = atts['retries'].to_i   or throw 'Missing retries'
      @remaining = atts['remaining'].to_i or throw 'Missing remaining'
      @failure   = atts['failure']        or {}
      @history   = atts['history']        or []
      # This is a silly side-effect of Lua doing JSON parsing
      if @tags == {}
        @tags = []
      end
      # Our lua scripts
      @put    = Lua.new('put'   , @redis)
      @track  = Lua.new('track' , @redis)
      @cancel = Lua.new('cancel', @redis)
    end
    
    def [](key)
      return @data[key]
    end
    
    def []=(key, val)
      return (@data[key] = val)
    end
    
    def to_s()
      return inspect()
    end
    
    def inspect()
      return "< Qless::Job " + @id + " >"
    end
    
    def remaining()
      return @expires - Time.now().to_i
    end
    
    # Move this from it's current queue into another
    def move(queue)
      return @put.call([queue], [
        @id, JSON.generate(@data), Time.now().to_i
      ])
    end
    
    def cancel()
      return @cancel.call([], [@id])
    end
    
    def track(*tags)
      return @track.call([], ['track', @id, Time.now().to_i] + tags)
    end
    
    def untrack()
      return @track.call([], ['untrack'], [@id, Time.now().to_i])
    end
  end  
end