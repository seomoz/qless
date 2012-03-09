require "qless/lua"
require "redis"
require "json"

module Qless
  class Stats    
    def initialize(redis)
      @get    = Lua.new('stats' , redis)
      @failed = Lua.new('failed', redis)
      @redis  = redis
    end
    
    def get(queue, date=nil)
      JSON.parse(@get.call([], [queue, (date or Time.now.to_i)]))
    end
    
    def failed(t=nil, start=0, limit=25)
      if not t
        return JSON.parse(@failed.call([], []))
      else
        return JSON.parse(@failed.call([], [t, start, limit]))
      end
    end
  end
end