require "socket"
require "redis"
require "json"

require "qless/version"
require "qless/config"
require "qless/queue"
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
      @config = Config.new(@redis)
      # Our lone Lua script
      @get     = Lua.new("get"    , @redis)
      @track   = Lua.new("track"  , @redis)
      @queues  = Lua.new("queues" , @redis)
      @failed  = Lua.new("failed" , @redis)
      @workers = Lua.new("workers", @redis)
    end
    
    def queue(name)
      Queue.new(name, @redis, @worker)
    end
    
    def queues(qname=nil)
      if qname.nil?
        JSON.parse(@queues.call([], [Time.now.to_i]))
      else
        JSON.parse(@queues.call([], [Time.now.to_i, qname]))
      end
    end
    
    def track(job)
      @track.call([], ['track', job.id, Time.now.to_i])
    end
    
    def untrack(jid)
      @track.call([], ['untrack', job.id, Time.now.to_i])
    end
    
    def tracked
      results = JSON.parse(@track.call([], []))
      results['jobs'] = results['jobs'].map { |j| Job.new(@redis, j) }
      results
    end
    
    def workers(worker=nil)
      if worker.nil?
        JSON.parse(@workers.call([], [Time.now.to_i]))
      else
        JSON.parse(@workers.call([], [Time.now.to_i, worker]))
      end
    end
    
    def failed(t=nil, start=0, limit=25)
      if not t
        JSON.parse(@failed.call([], []))
      else
        results = JSON.parse(@failed.call([], [t, start, limit]))
        results['jobs'] = results['jobs'].map { |j| Job.new(@redis, j) }
        results
      end
    end
    
    def job(id)
      results = @get.call([], [id])
      if results.nil?
        return nil
      end
      Job.new(@redis, JSON.parse(results))
    end
  end
end