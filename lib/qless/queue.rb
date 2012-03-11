require "qless/lua"
require "qless/job"
require "redis"
require "json"
require "uuid"

module Qless  
  # A configuration class associated with a qless client
  class Queue
    @@uuid = UUID.new
    attr_reader   :name, :worker
    attr_accessor :hb
    
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
      return @put.call([@name], [
        @@uuid.generate(:compact),
        JSON.generate(data),
        Time.now().to_i,
        (options[:priority] or 0),
        JSON.generate((options[:tags] or [])),
        (options[:delay] or 0)
      ])
    end
    
    # Pop a work item off the queue
    def pop(count=nil)
      results = @pop.call([@name], [
        @worker, (count or 1), Time.now().to_i, Time.now().to_i + @hb
      ]).map { |j| Job.new(@redis, JSON.parse(j)) }
      if count.nil?
        return results[0]
      end
      return results
    end
    
    # Peek at a work item
    def peek(count=nil)
      results = @peek.call([@name], [(count or 1), Time.now().to_i]).map { |j| Job.new(@redis, JSON.parse(j)) }
      if count.nil?
        return results[0]
      end
      return results
    end
    
    # Fail a job
    def fail(job, t, message)
      return @fail.call([], [
        job.id,
        @worker,
        t, message,
        Time.now().to_i,
        JSON.generate(job.data)])
    end
    
    # Heartbeat a job
    def heartbeat(job)
      return (@heartbeat.call([], [
        job.id,
        @worker,
        Time.now().to_i + @hb,
        JSON.generate(job.data)]) or 0).to_f
    end
    
    # Complete a job
    # Options include
    # => next (String) the next queue
    # => delay (int) how long to delay it in the next queue
    def complete(job, options={})
      if options[:next].nil?
        return @complete.call([], [
          job.id, @worker, @name, Time.now().to_i, JSON.generate(job.data),
          options[:next], (options[:delay] or 0)])
      else
        return @complete.call([], [job.id, @worker, @name, Time.now().to_i,
          JSON.generate(job.data)])
      end
    end
    
    def stats(date=nil)
      return JSON.parse(@stats.call([], [@name, (date or Time.now().to_i)]))
    end
    
    # How many items in the queue?
    def length()
      throw 'Unimplemented'
    end
  end
end
