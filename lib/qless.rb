require "socket"
require "redis"
require "json"
require "securerandom"

require "qless/version"
require "qless/config"
require "qless/queue"
require "qless/job"
require "qless/lua"

module Qless
  extend self

  def generate_jid
    SecureRandom.uuid.gsub('-', '')
  end

  def stringify_hash_keys(hash)
    hash.each_with_object({}) do |(key, value), result|
      result[key.to_s] = value
    end
  end

  class Client
    # Lua scripts
    attr_reader :_cancel, :_complete, :_fail, :_failed, :_get, :_getconfig, :_heartbeat, :_jobs, :_peek, :_pop, :_put, :_queues, :_setconfig, :_stats, :_track, :_workers, :_depends
    # A real object
    attr_reader :config, :redis, :worker
    
    def initialize(options = {})
      # This is a unique identifier for the worker
      @worker = Socket.gethostname + "-" + Process.pid.to_s
      # This is the redis instance we're connected to
      @redis  = Redis.new(options)
      @config = Config.new(self)
      ['cancel', 'complete', 'depends', 'fail', 'failed', 'get', 'getconfig', 'heartbeat', 'jobs',
        'peek', 'pop', 'put', 'queues', 'setconfig', 'stats', 'track', 'workers'].each do |f|
        self.instance_variable_set("@_#{f}", Lua.new(f, @redis))
      end
    end
    
    def queue(name)
      Queue.new(name, self, @worker)
    end
    
    def queues(qname=nil)
      if qname.nil?
        JSON.parse(@_queues.call([], [Time.now.to_i]))
      else
        JSON.parse(@_queues.call([], [Time.now.to_i, qname]))
      end
    end
    
    def track(job)
      @_track.call([], ['track', job.id, Time.now.to_i])
    end
    
    def untrack(jid)
      @_track.call([], ['untrack', job.id, Time.now.to_i])
    end
    
    def tracked
      results = JSON.parse(@_track.call([], []))
      results['jobs'] = results['jobs'].map { |j| Job.new(self, j) }
      results
    end
    
    def workers(worker=nil)
      if worker.nil?
        JSON.parse(@_workers.call([], [Time.now.to_i]))
      else
        JSON.parse(@_workers.call([], [Time.now.to_i, worker]))
      end
    end
    
    def failed(t=nil, start=0, limit=25)
      if not t
        JSON.parse(@_failed.call([], []))
      else
        results = JSON.parse(@_failed.call([], [t, start, limit]))
        results['jobs'] = results['jobs'].map { |j| Job.new(self, j) }
        results
      end
    end
    
    def job(id)
      results = @_get.call([], [id])
      if results.nil?
        return nil
      end
      Job.new(self, JSON.parse(results))
    end
  end
end
