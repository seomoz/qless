require "qless/job"
require "redis"
require "json"

module Qless
  class QueueJobs
    def initialize(name, client)
      @name   = name
      @client = client
    end

    def running(start=0, count=25)
      @client.call('jobs', 'running', @name, start, count)
    end

    def stalled(start=0, count=25)
      @client.call('jobs', 'stalled', @name, start, count)
    end

    def scheduled(start=0, count=25)
      @client.call('jobs', 'scheduled', @name, start, count)
    end

    def depends(start=0, count=25)
      @client.call('jobs', 'depends', @name, start, count)
    end

    def recurring(start=0, count=25)
      @client.call('jobs', 'recurring', @name, start, count)
    end
  end

  class Queue
    attr_reader   :name, :client
    attr_accessor :worker_name

    def initialize(name, client)
      @client = client
      @name   = name
      self.worker_name = Qless.worker_name
    end

    def jobs
      @jobs ||= QueueJobs.new(@name, @client)
    end

    def counts
      JSON.parse(@client.call('queues', @name))
    end

    def heartbeat
      get_config :heartbeat
    end

    def heartbeat=(value)
      set_config :heartbeat, value
    end

    def max_concurrency
      value = get_config(:"max-concurrency")
      value && Integer(value)
    end

    def max_concurrency=(value)
      set_config :"max-concurrency", value
    end

    def pause(opts={})
      @client.call('pause', name)
      if !opts[:stopjobs].nil? then
        @client.call('timeout', jobs.running(0, -1))
      end
    end

    def unpause
      @client.call('unpause', name)
    end

    # Put the described job in this queue
    # Options include:
    # => priority (int)
    # => tags (array of strings)
    # => delay (int)
    def put(klass, data, opts={})
      opts = job_options(klass, data, opts)

      @client.call('put', @name,
        (opts[:jid] or Qless.generate_jid),
        (klass.is_a? String) ? klass : klass.name,
        JSON.generate(data),
        opts.fetch(:delay, 0),
        'priority', opts.fetch(:priority, 0),
        'tags', JSON.generate(opts.fetch(:tags, [])),
        'retries', opts.fetch(:retries, 5),
        'depends', JSON.generate(opts.fetch(:depends, []))
      )
    end

    # Make a recurring job in this queue
    # Options include:
    # => priority (int)
    # => tags (array of strings)
    # => retries (int)
    # => offset (int)
    def recur(klass, data, interval, opts={})
      opts = job_options(klass, data, opts)
      @client.call(
        'recur',
        @name,
        (opts[:jid] or Qless.generate_jid),
        (klass.is_a? String) ? klass : klass.name,
        JSON.generate(data),
        'interval', interval, opts.fetch(:offset, 0),
        'priority', opts.fetch(:priority, 0),
        'tags', JSON.generate(opts.fetch(:tags, [])),
        'retries', opts.fetch(:retries, 5),
        'backlog', opts.fetch(:backlog, 0)
      )
    end

    # Pop a work item off the queue
    def pop(count=nil)
      results = JSON.parse(@client.call('pop', @name, worker_name, (count || 1))).map { |j| Job.new(@client, j) }
      count.nil? ? results[0] : results
    end

    # Peek at a work item
    def peek(count=nil)
      results = JSON.parse(@client.call('peek', @name, (count || 1))).map { |j| Job.new(@client, j) }
      count.nil? ? results[0] : results
    end

    def stats(date=nil)
      JSON.parse(@client.call('stats', @name, (date || Time.now.to_f)))
    end

    # How many items in the queue?
    def length
      (@client.redis.multi do
        @client.redis.zcard("ql:q:#{@name}-locks")
        @client.redis.zcard("ql:q:#{@name}-work")
        @client.redis.zcard("ql:q:#{@name}-scheduled")
      end).inject(0, :+)
    end

    def to_s
      "#<Qless::Queue #{@name}>"
    end
    alias inspect to_s

  private

    def job_options(klass, data, opts)
      return opts unless klass.respond_to?(:default_job_options)
      klass.default_job_options(data).merge(opts)
    end

    def set_config(config, value)
      @client.config["#{@name}-#{config}"] = value
    end

    def get_config(config)
      @client.config["#{@name}-#{config}"]
    end
  end
end
