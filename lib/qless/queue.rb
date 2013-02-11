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
      @client._jobs.call([], ['running', Time.now.to_f, @name, start, count])
    end

    def stalled(start=0, count=25)
      @client._jobs.call([], ['stalled', Time.now.to_f, @name, start, count])
    end

    def scheduled(start=0, count=25)
      @client._jobs.call([], ['scheduled', Time.now.to_f, @name, start, count])
    end

    def depends(start=0, count=25)
      @client._jobs.call([], ['depends', Time.now.to_f, @name, start, count])
    end

    def recurring(start=0, count=25)
      @client._jobs.call([], ['recurring', Time.now.to_f, @name, start, count])
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
      JSON.parse(@client._queues.call([], [Time.now.to_i, @name]))
    end

    def heartbeat
      @client.config["#{@name}-heartbeat"]
    end

    def heartbeat=(value)
      @client.config["#{@name}-heartbeat"] = value
    end

    def pause
      @client._pause.call([], [name])
    end

    def unpause
      @client._unpause.call([], [name])
    end

    # Put the described job in this queue
    # Options include:
    # => priority (int)
    # => tags (array of strings)
    # => delay (int)
    def put(klass, data, opts={})
      opts = job_options(klass, data, opts)

      @client._put.call([@name], [
        (opts[:jid] or Qless.generate_jid),
        klass.name,
        JSON.generate(data),
        Time.now.to_f,
        opts.fetch(:delay, 0),
        'priority', opts.fetch(:priority, 0),
        'tags', JSON.generate(opts.fetch(:tags, [])),
        'retries', opts.fetch(:retries, 5),
        'depends', JSON.generate(opts.fetch(:depends, []))
      ])
    end

    # Make a recurring job in this queue
    # Options include:
    # => priority (int)
    # => tags (array of strings)
    # => retries (int)
    # => offset (int)
    def recur(klass, data, interval, opts={})
      opts = job_options(klass, data, opts)

      @client._recur.call([], [
        'on',
        @name,
        (opts[:jid] or Qless.generate_jid),
        klass.to_s,
        JSON.generate(data),
        Time.now.to_f,
        'interval', interval, opts.fetch(:offset, 0),
        'priority', opts.fetch(:priority, 0),
        'tags', JSON.generate(opts.fetch(:tags, [])),
        'retries', opts.fetch(:retries, 5)
      ])
    end

    # Pop a work item off the queue
    def pop(count=nil)
      results = @client._pop.call([@name], [worker_name, (count || 1), Time.now.to_f]).map { |j| Job.new(@client, JSON.parse(j)) }
      count.nil? ? results[0] : results
    end

    # Peek at a work item
    def peek(count=nil)
      results = @client._peek.call([@name], [(count || 1), Time.now.to_f]).map { |j| Job.new(@client, JSON.parse(j)) }
      count.nil? ? results[0] : results
    end

    def stats(date=nil)
      JSON.parse(@client._stats.call([], [@name, (date || Time.now.to_f)]))
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
