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
      @client.config["#{@name}-heartbeat"]
    end

    def heartbeat=(value)
      @client.config["#{@name}-heartbeat"] = value
    end

    def pause
      @client.call('pause', name)
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
        klass.name,
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
        klass.to_s,
        JSON.generate(data),
        'interval', interval, opts.fetch(:offset, 0),
        'priority', opts.fetch(:priority, 0),
        'tags', JSON.generate(opts.fetch(:tags, [])),
        'retries', opts.fetch(:retries, 5)
      )
    end

    # Pop a work item off the queue
    def pop(count=nil)
      results = @client.call('pop', @name, worker_name, (count || 1)).map { |j| Job.new(@client, JSON.parse(j)) }
      count.nil? ? results[0] : results
    end

    # Peek at a work item
    def peek(count=nil)
      results = @client.call('peek', @name, (count || 1)).map { |j| Job.new(@client, JSON.parse(j)) }
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
  end
end
