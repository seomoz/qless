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

  # This is a unique identifier for the worker
  def worker_name
    @worker_name ||= [Socket.gethostname, Process.pid.to_s].join('-')
  end

  class UnsupportedRedisVersionError < StandardError; end

  class Client
    # Lua scripts
    attr_reader :_cancel, :_complete, :_fail, :_failed, :_get, :_getconfig, :_heartbeat, :_jobs, :_peek, :_pop
    attr_reader :_priority, :_put, :_queues, :_recur, :_retry, :_setconfig, :_stats, :_tag, :_track, :_workers, :_depends
    # A real object
    attr_reader :config, :redis
    
    def initialize(options = {})
      # This is the redis instance we're connected to
      @redis  = Redis.connect(options) # use connect so REDIS_URL will be honored
      # assert_minimum_redis_version("2.6")
      @config = Config.new(self)
      ['cancel', 'complete', 'depends', 'fail', 'failed', 'get', 'getconfig', 'heartbeat', 'jobs', 'peek', 'pop',
        'priority', 'put', 'queues', 'recur', 'retry', 'setconfig', 'stats', 'tag', 'track', 'workers'].each do |f|
        self.instance_variable_set("@_#{f}", Lua.new(f, @redis))
      end
    end
    
    def queue(name)
      Queue.new(name, self)
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
    
    def tagged(tag, offset=0, count=25)
      JSON.parse(@_tag.call([], ['get', tag, offset, count]))
    end
    
    def tags(offset=0, count=100)
      JSON.parse(@_tag.call([], ['top', offset, count]))
    end
    
    def complete(offset=0, count=25)
      @_jobs.call([], ['complete', offset, count])
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
        results = @_recur.call([], ['get', id])
        if results.nil?
          return nil
        end
        return RecurringJob.new(self, JSON.parse(results))
      end
      Job.new(self, JSON.parse(results))
    end

  private

    def assert_minimum_redis_version(version)
      redis_version = @redis.info.fetch("redis_version")
      return if Gem::Version.new(redis_version) >= Gem::Version.new(version)

      raise UnsupportedRedisVersionError,
        "You are running redis #{redis_version}, but qless requires at least #{version}"
    end
  end
end
