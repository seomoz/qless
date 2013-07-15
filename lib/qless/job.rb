require "qless"
require "qless/queue"
require "qless/lua_script"
require "redis"
require "json"

module Qless
  class BaseJob
    attr_reader :client

    def initialize(client, jid)
      @client = client
      @jid    = jid
    end

    def klass
      @klass ||= @klass_name.split('::').inject(Object) { |context, name| context.const_get(name) }
    end

    def queue
      @queue ||= Queue.new(@queue_name, @client)
    end
  end

  class Job < BaseJob
    attr_reader :jid, :expires_at, :state, :queue_name, :worker_name, :failure, :klass_name, :tracked, :dependencies, :dependents
    attr_reader :original_retries, :retries_left, :raw_queue_history
    attr_accessor :data, :priority, :tags

    MiddlewareMisconfiguredError = Class.new(StandardError)

    module SupportsMiddleware
      def around_perform(job)
        perform(job)
      end
    end

    def perform
      middlewares = Job.middlewares_on(klass)

      if middlewares.last == SupportsMiddleware
        klass.around_perform(self)
      elsif middlewares.any?
        raise MiddlewareMisconfiguredError, "The middleware chain for #{klass} " +
          "(#{middlewares.inspect}) is misconfigured. Qless::Job::SupportsMiddleware " +
          "must be extended onto your job class first if you want to use any middleware."
      else
        klass.perform(self)
      end
    end

    def self.build(client, klass, attributes = {})
      defaults = {
        "jid"              => Qless.generate_jid,
        "data"             => {},
        "klass"            => klass.to_s,
        "priority"         => 0,
        "tags"             => [],
        "worker"           => "mock_worker",
        "expires"          => Time.now + (60 * 60), # an hour from now
        "state"            => "running",
        "tracked"          => false,
        "queue"            => "mock_queue",
        "retries"          => 5,
        "remaining"        => 5,
        "failure"          => {},
        "history"          => [],
        "dependencies"     => [],
        "dependents"       => []
      }
      attributes = defaults.merge(Qless.stringify_hash_keys(attributes))
      attributes["data"] = JSON.dump(attributes["data"])
      new(client, attributes)
    end

    def self.middlewares_on(job_klass)
      job_klass.singleton_class.ancestors.select do |ancestor|
        ancestor.method_defined?(:around_perform)
      end
    end

    def initialize(client, atts)
      super(client, atts.fetch('jid'))
      %w{jid data priority tags state tracked
        failure dependencies dependents}.each do |att|
        self.instance_variable_set("@#{att}".to_sym, atts.fetch(att))
      end

      # Parse the data string
      @data = JSON.parse(@data)

      @expires_at        = atts.fetch('expires')
      @klass_name        = atts.fetch('klass')
      @queue_name        = atts.fetch('queue')
      @worker_name       = atts.fetch('worker')
      @original_retries  = atts.fetch('retries')
      @retries_left      = atts.fetch('remaining')
      @raw_queue_history = atts.fetch('history')

      # This is a silly side-effect of Lua doing JSON parsing
      @tags         = [] if @tags == {}
      @dependents   = [] if @dependents == {}
      @dependencies = [] if @dependencies == {}
      @state_changed = false
      @before_callbacks = Hash.new { |h, k| h[k] = [] }
      @after_callbacks  = Hash.new { |h, k| h[k] = [] }
    end

    def priority=(priority)
      if @client.call('priority', @jid, priority)
        @priority = priority
      end
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

    def description
      "#{@klass_name} (#{@jid} / #{@queue_name} / #{@state})"
    end

    def inspect
      "<Qless::Job #{description}>"
    end

    def ttl
      @expires_at - Time.now.to_f
    end

    def reconnect_to_redis
      @client.redis.client.reconnect
    end

    def history
      warn "WARNING: Qless::Job#history is deprecated; use Qless::Job#raw_queue_history instead" +
           "; called from:\n#{caller.first}\n"
      raw_queue_history
    end

    def queue_history
      @queue_history ||= @raw_queue_history.map do |history_event|
        history_event.each_with_object({}) do |(key, value), hash|
          # The only Numeric (Integer or Float) values we get in the history are timestamps
          hash[key] = if value.is_a?(Numeric)
            Time.at(value).utc
          else
            value
          end
        end
      end
    end

    def initially_put_at
      @initially_put_at ||= history_timestamp('put', :min)
    end

    def to_hash
      {
        jid: jid,
        expires_at: expires_at,
        state: state,
        queue_name: queue_name,
        history: raw_queue_history,
        worker_name: worker_name,
        failure: failure,
        klass_name: klass_name,
        tracked: tracked,
        dependencies: dependencies,
        dependents: dependents,
        original_retries: original_retries,
        retries_left: retries_left,
        data: data,
        priority: priority,
        tags: tags
      }
    end

    # Move this from it's current queue into another
    def move(queue, opts={})
      note_state_change :move do
        @client.call('put', queue, @jid, @klass_name,
          JSON.generate(opts.fetch(:data, @data)),
          opts.fetch(:delay, 0),
          'priority', opts.fetch(:priority, @priority),
          'tags', JSON.generate(opts.fetch(:tags, @tags)),
          'retries', opts.fetch(:retries, @original_retries),
          'depends', JSON.generate(opts.fetch(:depends, @dependencies))
        )
      end
    end

    CantFailError = Class.new(Qless::LuaScriptError)

    # Fail a job
    def fail(group, message)
      note_state_change :fail do
        @client.call(
          'fail',
          @jid,
          @worker_name,
          group, message,
          JSON.generate(@data)) || false
      end
    rescue Qless::LuaScriptError => err
      raise CantFailError.new(err.message)
    end

    # Heartbeat a job
    def heartbeat()
      @client.call(
        'heartbeat',
        @jid,
        @worker_name,
        JSON.generate(@data)) || false
    end

    CantCompleteError = Class.new(Qless::LuaScriptError)

    # Complete a job
    # Options include
    # => next (String) the next queue
    # => delay (int) how long to delay it in the next queue
    def complete(nxt=nil, options={})
      note_state_change :complete do
        if nxt.nil?
          @client.call(
            'complete', @jid, @worker_name, @queue_name, JSON.generate(@data))
        else
          @client.call('complete',
            @jid, @worker_name, @queue_name, JSON.generate(@data), 'next', nxt, 'delay',
            options.fetch(:delay, 0), 'depends', JSON.generate(options.fetch(:depends, [])))
        end
      end
    rescue Qless::LuaScriptError => err
      raise CantCompleteError.new(err.message)
    end

    def state_changed?
      @state_changed
    end

    def cancel
      note_state_change :cancel do
        @client.call('cancel', @jid)
      end
    end

    def track()
      @client.call('track', 'track', @jid)
    end

    def untrack
      @client.call('track', 'untrack', @jid)
    end

    def tag(*tags)
      @client.call('tag', 'add', @jid, *tags)
    end

    def untag(*tags)
      @client.call('tag', 'remove', @jid, *tags)
    end

    def retry(delay=0, group=nil, message=nil)
      note_state_change :retry do
        if group.nil?
          results = @client.call(
            'retry', @jid, @queue_name, @worker_name, delay)
          results.nil? ? false : results
        else
          results = @client.call(
            'retry', @jid, @queue_name, @worker_name, delay, group, message)
          results.nil? ? false : results
        end
      end
    end

    def depend(*jids)
      !!@client.call('depends', @jid, 'on', *jids)
    end

    def undepend(*jids)
      !!@client.call('depends', @jid, 'off', *jids)
    end

    def timeout()
      @client.call('timeout', @jid)
    end

    def log(message, data=nil)
      if data then
        @client.call('log', @jid, message, JSON.generate(data))
      else
        @client.call('log', @jid, message)
      end
    end

    [:fail, :complete, :cancel, :move, :retry].each do |event|
      define_method :"before_#{event}" do |&block|
        @before_callbacks[event] << block
      end

      define_method :"after_#{event}" do |&block|
        @after_callbacks[event].unshift block
      end
    end

    def note_state_change(event)
      @before_callbacks[event].each { |blk| blk.call(self) }
      result = yield
      @state_changed = true
      @after_callbacks[event].each { |blk| blk.call(self) }
      result
    end

  private

    def history_timestamp(name, selector)
      queue_history.select { |q|
        q["what"] == name
      }.map { |q|
        q["when"]
      }.public_send(selector)
    end
  end

  class RecurringJob < BaseJob
    attr_reader :jid, :data, :priority, :tags, :retries, :interval, :count, :queue_name, :klass_name, :backlog

    def initialize(client, atts)
      super(client, atts.fetch('jid'))
      %w{jid data priority tags retries interval count backlog}.each do |att|
        self.instance_variable_set("@#{att}".to_sym, atts.fetch(att))
      end

      # Parse the data string
      @data        = JSON.parse(@data)
      @klass_name  = atts.fetch('klass')
      @queue_name  = atts.fetch('queue')
      @tags        = [] if @tags == {}
    end

    def priority=(value)
      @client.call('recur.update', @jid, 'priority', value)
      @priority = value
    end

    def retries=(value)
      @client.call('recur.update', @jid, 'retries', value)
      @retries = value
    end

    def interval=(value)
      @client.call('recur.update', @jid, 'interval', value)
      @interval = value
    end

    def data=(value)
      @client.call('recur.update', @jid, 'data', JSON.generate(value))
      @data = value
    end

    def klass=(value)
      @client.call('recur.update', @jid, 'klass', value.to_s)
      @klass_name = value.to_s
    end

    def backlog=(value)
      @client.call('recur.update', @jid, 'backlog', value.to_s)
      @backlog = value
    end

    def move(queue)
      @client.call('recur.update', @jid, 'queue', queue)
      @queue_name = queue
    end

    def cancel
      @client.call('unrecur', @jid)
    end

    def tag(*tags)
      @client.call('recur.tag', @jid, *tags)
    end

    def untag(*tags)
      @client.call('recur.untag', @jid, *tags)
    end
  end
end
