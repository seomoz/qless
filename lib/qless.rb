require "socket"
require "redis"
require "json"
require "securerandom"

module Qless
  # Define our error base class before requiring the other
  # files so they can define subclasses.
  Error = Class.new(StandardError)

  # to maintain backwards compatibility with v2.x of that gem we need this constant because:
  # * (lua.rb) the #evalsha method signature changed between v2.x and v3.x of the redis ruby gem
  # * (worker.rb) in v3.x you have to reconnect to the redis server after forking the process
  USING_LEGACY_REDIS_VERSION = ::Redis::VERSION.to_f < 3.0
end

require "qless/version"
require "qless/config"
require "qless/queue"
require "qless/job"
require "qless/lua_script"

module Qless
  extend self

  UnsupportedRedisVersionError = Class.new(Error)

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

  class ClientJobs
    def initialize(client)
      @client = client
    end

    def complete(offset=0, count=25)
      @client._jobs.call([], ['complete', offset, count])
    end

    def tracked
      results = JSON.parse(@client._track.call([], []))
      results['jobs'] = results['jobs'].map { |j| Job.new(@client, j) }
      results
    end

    def tagged(tag, offset=0, count=25)
      JSON.parse(@client._tag.call([], ['get', tag, offset, count]))
    end

    def failed(t=nil, start=0, limit=25)
      if not t
        JSON.parse(@client._failed.call([], []))
      else
        results = JSON.parse(@client._failed.call([], [t, start, limit]))
        results['jobs'] = results['jobs'].map { |j| Job.new(@client, j) }
        results
      end
    end

    def [](id)
      results = @client._get.call([], [id])
      if results.nil?
        results = @client._recur.call([], ['get', id])
        if results.nil?
          return nil
        end
        return RecurringJob.new(@client, JSON.parse(results))
      end
      Job.new(@client, JSON.parse(results))
    end
  end

  class ClientWorkers
    def initialize(client)
      @client = client
    end

    def counts
      JSON.parse(@client._workers.call([], [Time.now.to_i]))
    end

    def [](name)
      JSON.parse(@client._workers.call([], [Time.now.to_i, name]))
    end
  end

  class ClientQueues
    def initialize(client)
      @client = client
    end

    def counts
      JSON.parse(@client._queues.call([], [Time.now.to_i]))
    end

    def [](name)
      Queue.new(name, @client)
    end
  end

  class ClientEvents
    def initialize(redis)
      @redis   = redis
      @actions = Hash.new()
    end

    def canceled(&block) ; @actions[:canceled ] = block; end
    def completed(&block); @actions[:completed] = block; end
    def failed(&block)   ; @actions[:failed   ] = block; end
    def popped(&block)   ; @actions[:popped   ] = block; end
    def stalled(&block)  ; @actions[:stalled  ] = block; end
    def put(&block)      ; @actions[:put      ] = block; end
    def track(&block)    ; @actions[:track    ] = block; end
    def untrack(&block)  ; @actions[:untrack  ] = block; end

    def listen
      yield(self) if block_given?
      @redis.subscribe(:canceled, :completed, :failed, :popped, :stalled, :put, :track, :untrack) do |on|
        on.message do |channel, message|
          callback = @actions[channel.to_sym]
          if not callback.nil?
            callback.call(message)
          end
        end
      end
    end

    def stop
      @redis.unsubscribe
    end
  end

  class Client
    # Lua scripts
    attr_reader :_cancel, :_config, :_complete, :_fail, :_failed, :_get, :_heartbeat, :_jobs, :_peek, :_pop
    attr_reader :_priority, :_put, :_queues, :_recur, :_retry, :_stats, :_tag, :_track, :_workers, :_depends
    attr_reader :_pause, :_unpause, :_deregister_workers
    # A real object
    attr_reader :config, :redis, :jobs, :queues, :workers

    def initialize(options = {})
      # This is the redis instance we're connected to
      @redis   = options[:redis] || Redis.connect(options) # use connect so REDIS_URL will be honored
      @options = options
      assert_minimum_redis_version("2.5.5")
      @config = Config.new(self)
      ['cancel', 'config', 'complete', 'depends', 'fail', 'failed', 'get', 'heartbeat', 'jobs', 'peek', 'pop',
        'priority', 'put', 'queues', 'recur', 'retry', 'stats', 'tag', 'track', 'workers', 'pause', 'unpause',
        'deregister_workers'].each do |f|
        self.instance_variable_set("@_#{f}", Qless::LuaScript.new(f, @redis))
      end

      @jobs    = ClientJobs.new(self)
      @queues  = ClientQueues.new(self)
      @workers = ClientWorkers.new(self)
    end

    def inspect
      "<Qless::Client #{@options} >"
    end

    def events
      # Events needs its own redis instance of the same configuration, because
      # once it's subscribed, we can only use pub-sub-like commands. This way,
      # we still have access to the client in the normal case
      @events ||= ClientEvents.new(Redis.connect(@options))
    end

    def track(jid)
      @_track.call([], ['track', jid, Time.now.to_i])
    end

    def untrack(jid)
      @_track.call([], ['untrack', jid, Time.now.to_i])
    end

    def tags(offset=0, count=100)
      JSON.parse(@_tag.call([], ['top', offset, count]))
    end

    def deregister_workers(*worker_names)
      _deregister_workers.call([], worker_names)
    end

    def bulk_cancel(jids)
      @_cancel.call([], jids)
    end

    def new_redis_connection
      ::Redis.new(url: redis.id)
    end

  private

    def assert_minimum_redis_version(version)
      # remove the "-pre2" from "2.6.8-pre2"
      redis_version = @redis.info.fetch("redis_version").split('-').first
      return if Gem::Version.new(redis_version) >= Gem::Version.new(version)

      raise UnsupportedRedisVersionError,
        "You are running redis #{redis_version}, but qless requires at least #{version}"
    end
  end
end
