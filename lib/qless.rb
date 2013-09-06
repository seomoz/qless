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
require "qless/failure_formatter"

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

  def failure_formatter
    @failure_formatter ||= FailureFormatter.new
  end

  class ClientJobs
    def initialize(client)
      @client = client
    end

    def complete(offset=0, count=25)
      @client.call('jobs', 'complete', offset, count)
    end

    def tracked
      results = JSON.parse(@client.call('track'))
      results['jobs'] = results['jobs'].map { |j| Job.new(@client, j) }
      results
    end

    def tagged(tag, offset=0, count=25)
      JSON.parse(@client.call('tag', 'get', tag, offset, count))
    end

    def failed(t=nil, start=0, limit=25)
      if not t
        JSON.parse(@client.call('failed'))
      else
        results = JSON.parse(@client.call('failed', t, start, limit))
        results['jobs'] = multiget(*results['jobs'])
        results
      end
    end

    def [](id)
      return get(id)
    end

    def get(jid)
      results = @client.call('get', jid)
      if results.nil?
        results = @client.call('recur.get', jid)
        if results.nil?
          return nil
        end
        return RecurringJob.new(@client, JSON.parse(results))
      end
      Job.new(@client, JSON.parse(results))
    end

    def multiget(*jids)
      results = JSON.parse(@client.call('multiget', *jids))
      results.map do |data|
        if data.nil?
          nil
        else
          Job.new(@client, data)
        end
      end
    end
  end

  class ClientWorkers
    def initialize(client)
      @client = client
    end

    def counts
      JSON.parse(@client.call('workers'))
    end

    def [](name)
      JSON.parse(@client.call('workers', name))
    end
  end

  class ClientQueues
    def initialize(client)
      @client = client
    end

    def counts
      JSON.parse(@client.call('queues'))
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
      @redis.subscribe('ql:canceled', 'ql:completed', 'ql:failed', 'ql:popped',
        'ql:stalled', 'ql:put', 'ql:track', 'ql:untrack') do |on|
        on.message do |channel, message|
          callback = @actions[channel.sub('ql:', '').to_sym]
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
    # Lua script
    attr_reader :_qless
    # A real object
    attr_reader :config, :redis, :jobs, :queues, :workers

    def initialize(options = {})
      # This is the redis instance we're connected to
      @redis   = options[:redis] || Redis.connect(options) # use connect so REDIS_URL will be honored
      @options = options
      assert_minimum_redis_version("2.5.5")
      @config = Config.new(self)
      @_qless = Qless::LuaScript.new('qless', @redis)

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

    def call(command, *argv)
      @_qless.call(command, Time.now.to_f, *argv)
    end

    def track(jid)
      call('track', jid)
    end

    def untrack(jid)
      call('untrack', jid)
    end

    def tags(offset=0, count=100)
      JSON.parse(call('tag', 'top', offset, count))
    end

    def deregister_workers(*worker_names)
      call('worker.deregister', *worker_names)
    end

    def bulk_cancel(jids)
      call('cancel', jids)
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
