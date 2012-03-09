require "socket"
require "redis"
require "json"

require "qless/version"
require "qless/config"
require "qless/queue"
require "qless/stats"
require "qless/job"
require "qless/lua"

module Qless
  class Client
    attr_reader :stats, :config
    
    def initialize(options = {})
      # This is a unique identifier for the worker
      @worker = Socket.gethostname + "-" + Process.pid.to_s
      # This is the redis instance we're connected to
      @redis  = Redis.new(options)
      @stats  = Stats.new(@redis)
      @config = Config.new(@redis)
      # Our lone Lua script
      @get    = Lua.new("get", @redis)
    end
    
    def queue(name)
      return Queue.new(name, @redis, @worker)
    end
    
    def job(id)
      results = @get.call([], [id])
      if results.nil?
        return nil
      end
      return Job.new(@redis, JSON.parse(results))
    end
  end
end