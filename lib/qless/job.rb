require "qless/lua"
require "redis"
require "json"

module Qless
  class Job
    attr_reader :id, :expires, :state, :queue, :history, :worker
    attr_accessor :data, :priority, :tags
    
    def initialize(redis, atts)
      @redis    = redis
      @id       = atts['id']       or throw 'Missing ID'
      @data     = atts['data']     or throw 'Missing user data'
      @priority = atts['priority'] or throw 'Missing priority'
      @tags     = atts['tags']     or throw 'Missing tags'
      @worker   = atts['worker']   or throw 'Missing worker'
      @expires  = atts['expires']  or throw 'Missing expires'
      @state    = atts['state']    or throw 'Missing state'
      @queue    = atts['queue']    or throw 'Missing queue'
      @history  = atts['history']  or []
      # Our lua scripts
      @put    = Lua.new('put'   , @redis)
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
      return Time.now().to_i - @expires
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
  end  
end