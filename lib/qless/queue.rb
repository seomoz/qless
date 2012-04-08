require "qless/lua"
require "qless/job"
require "redis"
require "json"
require "uuid"
require "securerandom"

module Qless  
  # A configuration class associated with a qless client
  class Queue
    @@uuid = UUID.new
    attr_reader   :name
    attr_accessor :hb, :worker
    
    def initialize(name, redis, worker, heartbeat=nil)
      @redis  = redis
      @name   = name
      @worker = worker
      # Our heartbeat rate, in seconds
      @hb     = (heartbeat or 60)
      # And now for our Lua scripts
      @put       = Lua.new('put'      , redis)
      @pop       = Lua.new('pop'      , redis)
      @fail      = Lua.new('fail'     , redis)
      @peek      = Lua.new('peek'     , redis)
      @jobs      = Lua.new('jobs'     , redis)
      @stats     = Lua.new('stats'    , redis)
      @complete  = Lua.new('complete' , redis)
      @heartbeat = Lua.new('heartbeat', redis)
    end
    
    # Put the described job in this queue
    # Options include:
    # => priority (int)
    # => tags (array of strings)
    # => delay (int)
    def put(data, options={})
      @put.call([@name], [
        @@uuid.generate(:compact),
        JSON.generate(data),
        Time.now.to_i,
        (options[:priority] || 0),
        JSON.generate((options[:tags] || [])),
        (options[:delay] || 0),
        (options[:retries] || 5)
      ])
    end
    
    # Pop a work item off the queue
    def pop(count=nil)
      results = @pop.call([@name], [
        @worker, (count || 1), Time.now.to_i, Time.now.to_i + @hb
      ]).map { |j| Job.new(@redis, JSON.parse(j)) }      
      count.nil? ? results[0] : results
    end
    
    # Peek at a work item
    def peek(count=nil)
      results = @peek.call([@name], [(count || 1), Time.now.to_i]).map { |j| Job.new(@redis, JSON.parse(j)) }
      count.nil? ? results[0] : results
    end
    
    # Fail a job
    def fail(job, t, message)
      @fail.call([], [
        job.id,
        @worker,
        t, message,
        Time.now.to_i,
        JSON.generate(job.data)]) || false
    end
    
    # Heartbeat a job
    def heartbeat(job)
      @heartbeat.call([], [
        job.id,
        @worker,
        Time.now.to_i + @hb,
        JSON.generate(job.data)]) || false
    end
    
    # Complete a job
    # Options include
    # => next (String) the next queue
    # => delay (int) how long to delay it in the next queue
    def complete(job, options={})
      if options[:next].nil?
        response = @complete.call([], [
          job.id, @worker, @name, Time.now.to_i, JSON.generate(job.data)])
      else
        response = @complete.call([], [
          job.id, @worker, @name, Time.now.to_i, JSON.generate(job.data),
          options[:next], (options[:delay] || 0)])
      end
      response.nil? ? false : response
    end
    
    def running
      @jobs.call([], ['running', Time.now.to_i, @name])
    end
    
    def stalled
      @jobs.call([], ['stalled', Time.now.to_i, @name])
    end
    
    def scheduled
      @jobs.call([], ['scheduled', Time.now.to_i, @name])
    end
    
    def stats(date=nil)
      JSON.parse(@stats.call([], [@name, (date || Time.now.to_i)]))
    end
    
    # How many items in the queue?
    def length
      (@redis.pipelined do
        @redis.zcard("ql:q:" + @name + "-locks")
        @redis.zcard("ql:q:" + @name + "-work")
        @redis.zcard("ql:q:" + @name + "-scheduled")
      end).inject(0, :+)
    end
  end
end
