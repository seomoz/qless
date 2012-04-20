require "qless/lua"
require "qless/job"
require "redis"
require "json"

module Qless  
  # A configuration class associated with a qless client
  class Queue
    attr_reader   :name
    attr_accessor :worker_name
    
    def initialize(name, client)
      @client = client
      @name   = name
      self.worker_name = Qless.worker_name
    end
    
    # Put the described job in this queue
    # Options include:
    # => priority (int)
    # => tags (array of strings)
    # => delay (int)
    def put(klass, data, opts={})
      @client._put.call([@name], [
        Qless.generate_jid,
        klass.to_s,
        JSON.generate(data),
        Time.now.to_f,
        opts.fetch(:delay, 0),
        'priority', opts.fetch(:priority, 0),
        'tags', JSON.generate(opts.fetch(:tags, [])),
        'retries', opts.fetch(:retries, 5),
        'depends', JSON.generate(opts.fetch(:depends, []))
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
    
    def stats(date=nil)
      JSON.parse(@client._stats.call([], [@name, (date || Time.now.to_f)]))
    end
    
    # How many items in the queue?
    def length
      (@client.redis.pipelined do
        @client.redis.zcard("ql:q:" + @name + "-locks")
        @client.redis.zcard("ql:q:" + @name + "-work")
        @client.redis.zcard("ql:q:" + @name + "-scheduled")
      end).inject(0, :+)
    end
  end
end
