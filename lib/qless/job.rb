require "qless/lua"
require "redis"
require "json"

module Qless
  class Job
    attr_reader :id, :expires, :state, :queue, :history, :worker, :retries, :remaining, :failure
    attr_accessor :data, :priority, :tags
    
    def initialize(redis, atts)
      @redis     = redis
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
      # Our lua scripts
      @put    = Lua.new('put'   , @redis)
      @track  = Lua.new('track' , @redis)
      @cancel = Lua.new('cancel', @redis)
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
      @put.call([queue], [
        @id, JSON.generate(@data), Time.now.to_i
      ])
    end
    
    def cancel
      @cancel.call([], [@id])
    end
    
    def track(*tags)
      @track.call([], ['track', @id, Time.now.to_i] + tags)
    end
    
    def untrack
      @track.call([], ['untrack', @id, Time.now.to_i])
    end
  end  
end