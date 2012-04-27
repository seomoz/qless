require "qless"
require "qless/lua"
require "redis"
require "json"

module Qless
  class Job
    attr_reader :jid, :expires, :state, :queue, :history, :worker_name, :retries, :remaining, :failure, :klass, :delay, :tracked, :dependencies, :dependents
    attr_accessor :data, :priority, :tags
    
    def perform
      klass = @klass.split('::').inject(Kernel) { |context, name| context.const_get(name) }
      klass.perform(self)
    end

    def self.build(client, klass, attributes = {})
      defaults = {
        "jid" => Qless.generate_jid,
        "data" => {},
        "klass" => klass.to_s,
        "priority" => 0,
        "tags" => [],
        "worker" => "mock_worker",
        "expires" => Time.now + (60 * 60), # an hour from now
        "state" => "running",
        "tracked" => false,
        "queue" => "mock_queue",
        "retries" => 5,
        "remaining" => 5,
        "failure" => {},
        "history" => [],
        "dependencies" => [],
        "dependents" => []
      }
      attributes = defaults.merge(Qless.stringify_hash_keys(attributes))
      new(client, attributes)
    end
    
    def initialize(client, atts)
      @client    = client
      %w{jid data klass priority tags expires state tracked queue
        retries remaining failure history dependencies dependents}.each do |att|
        self.instance_variable_set("@#{att}".to_sym, atts.fetch(att))
      end
      @delay = atts.fetch('delay', 0)
      @worker_name = atts.fetch('worker')

      # This is a silly side-effect of Lua doing JSON parsing
      @tags         = [] if @tags == {}
      @dependents   = [] if @dependents == {}
      @dependencies = [] if @dependencies == {}
      @state_changed = false
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
      "#{@jid} (#{@klass} / #{@queue})"
    end
    
    def inspect
      "<Qless::Job #{description}>"
    end
    
    def ttl
      @expires - Time.now.to_f
    end
    
    # Move this from it's current queue into another
    def move(queue)
      note_state_change do
        @client._put.call([queue], [
          @jid, @klass, JSON.generate(@data), Time.now.to_f, 0
        ])
      end
    end
    
    # Fail a job
    def fail(group, message)
      note_state_change do
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
    
    # Complete a job
    # Options include
    # => next (String) the next queue
    # => delay (int) how long to delay it in the next queue
    def complete(nxt=nil, options={})
      response = note_state_change do
        if nxt.nil?
          @client._complete.call([], [
            @jid, @worker_name, @queue, Time.now.to_f, JSON.generate(@data)])
        else
          @client._complete.call([], [
            @jid, @worker_name, @queue, Time.now.to_f, JSON.generate(@data), 'next', nxt, 'delay',
            options.fetch(:delay, 0), 'depends', JSON.generate(options.fetch(:depends, []))])
        end
      end
      response.nil? ? false : response
    end

    def state_changed?
      @state_changed
    end
    
    def cancel
      note_state_change do
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
      results = @client._retry.call([], [@jid, @queue, @worker_name, Time.now.to_f, delay])
      results.nil? ? false : results
    end
    
    def depend(*jids)
      !!@client._depends.call([], [@jid, 'on'] + jids)
    end
    
    def undepend(*jids)
      !!@client._depends.call([], [@jid, 'off'] + jids)
    end

  private

    def note_state_change
      result = yield
      @state_changed = true
      result
    end
  end  
end
