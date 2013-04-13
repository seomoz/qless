require "qless"
require "qless/queue"
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
      attributes["data"] = JSON.parse(JSON.dump attributes["data"])
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
      if @client._priority.call([], [@jid, priority])
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
    def move(queue)
      note_state_change :move do
        @client._put.call([queue], [
          @jid, @klass_name, JSON.generate(@data), Time.now.to_f, 0
        ])
      end
    end

    # Fail a job
    def fail(group, message)
      note_state_change :fail do
        @client._fail.call([], [
          @jid,
          @worker_name,
          group, message,
          Time.now.to_f,
          JSON.generate(@data)]) || false
      end
    end

    # Heartbeat a job
    def heartbeat()
      @client._heartbeat.call([], [
        @jid,
        @worker_name,
        Time.now.to_f,
        JSON.generate(@data)]) || false
    end

    CantCompleteError = Class.new(Qless::Error)

    # Complete a job
    # Options include
    # => next (String) the next queue
    # => delay (int) how long to delay it in the next queue
    def complete(nxt=nil, options={})
      note_state_change :complete do
        response = if nxt.nil?
          @client._complete.call([], [
            @jid, @worker_name, @queue_name, Time.now.to_f, JSON.generate(@data)])
        else
          @client._complete.call([], [
            @jid, @worker_name, @queue_name, Time.now.to_f, JSON.generate(@data), 'next', nxt, 'delay',
            options.fetch(:delay, 0), 'depends', JSON.generate(options.fetch(:depends, []))])
        end

        if response
          response
        else
          description = if reloaded_instance = @client.jobs[@jid]
                          reloaded_instance.description
                        else
                          self.description + " -- can't be reloaded"
                        end

          raise CantCompleteError, "Failed to complete #{description}"
        end
      end
    end

    def state_changed?
      @state_changed
    end

    def cancel
      note_state_change :cancel do
        @client._cancel.call([], [@jid])
      end
    end

    def track()
      @client._track.call([], ['track', @jid, Time.now.to_f])
    end

    def untrack
      @client._track.call([], ['untrack', @jid, Time.now.to_f])
    end

    def tag(*tags)
      @client._tag.call([], ['add', @jid, Time.now.to_f] + tags)
    end

    def untag(*tags)
      @client._tag.call([], ['remove', @jid, Time.now.to_f] + tags)
    end

    def retry(delay=0)
      note_state_change :retry do
        results = @client._retry.call([], [@jid, @queue_name, @worker_name, Time.now.to_f, delay])
        results.nil? ? false : results
      end
    end

    def depend(*jids)
      !!@client._depends.call([], [@jid, 'on'] + jids)
    end

    def undepend(*jids)
      !!@client._depends.call([], [@jid, 'off'] + jids)
    end

    [:fail, :complete, :cancel, :move, :retry].each do |event|
      define_method :"before_#{event}" do |&block|
        @before_callbacks[event] << block
      end

      define_method :"after_#{event}" do |&block|
        @after_callbacks[event].unshift block
      end
    end

  private

    def note_state_change(event)
      @before_callbacks[event].each { |blk| blk.call(self) }
      result = yield
      @state_changed = true
      @after_callbacks[event].each { |blk| blk.call(self) }
      result
    end

    def history_timestamp(name, selector)
      queue_history.map { |q| q[name] }.compact.send(selector)
    end
  end

  class RecurringJob < BaseJob
    attr_reader :jid, :data, :priority, :tags, :retries, :interval, :count, :queue_name, :klass_name

    def initialize(client, atts)
      super(client, atts.fetch('jid'))
      %w{jid data priority tags retries interval count}.each do |att|
        self.instance_variable_set("@#{att}".to_sym, atts.fetch(att))
      end

      @klass_name  = atts.fetch('klass')
      @queue_name  = atts.fetch('queue')
      @tags        = [] if @tags == {}
    end

    def priority=(value)
      @client._recur.call([], ['update', @jid, 'priority', value])
      @priority = value
    end

    def retries=(value)
      @client._recur.call([], ['update', @jid, 'retries', value])
      @retries = value
    end

    def interval=(value)
      @client._recur.call([], ['update', @jid, 'interval', value])
      @interval = value
    end

    def data=(value)
      @client._recur.call([], ['update', @jid, 'data', JSON.generate(value)])
      @data = value
    end

    def klass=(value)
      @client._recur.call([], ['update', @jid, 'klass', value.to_s])
      @klass_name = value.to_s
    end

    def move(queue)
      @client._recur.call([], ['update', @jid, 'queue', queue])
      @queue_name = queue
    end

    def cancel
      @client._recur.call([], ['off', @jid])
    end

    def tag(*tags)
      @client._recur.call([], ['tag', @jid] + tags)
    end

    def untag(*tags)
      @client._recur.call([], ['untag', @jid] + tags)
    end
  end
end
