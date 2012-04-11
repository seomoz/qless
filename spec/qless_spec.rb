require 'spec_helper'
require "qless"
require "redis"
require "json"
require 'yaml'

module Qless
  class FooJob
    # An empty class
  end
  
  describe Qless::Client do
    # Our main client
    let(:client) { Qless::Client.new(redis_config) }
    # Our main test queue
    let(:q) { client.queue("testing") }
    # Point to the main queue, but identify as different workers
    let(:a) { client.queue("testing").tap { |o| o.worker = "worker-a" } }
    let(:b) { client.queue("testing").tap { |o| o.worker = "worker-b" } }
    # And a second queue
    let(:other) { client.queue("other")   }

    let(:redis_config) do
      if File.exist?('./spec/redis.config.yml')
        YAML.load_file('./spec/redis.config.yml')
      else
        {}
      end
    end

    def assert_minimum_redis_version(version)
      redis_version = Gem::Version.new(@redis.info["redis_version"])
      if redis_version < Gem::Version.new(version)
        pending "You are running redis #{redis_version}, but qless requires at least #{version}"
      end
    end

    before(:each) do
      # Sometimes we need raw redis access
      @redis = Redis.new(redis_config)
      assert_minimum_redis_version("2.6")
      if @redis.keys("*").length > 0
        raise "Must start with empty Redis DB"
      end
      @redis.script(:flush)
    end
    
    after(:each) do
      @redis.flushdb
    end
    
    describe "#config" do
      it "can set, get and erase configuration" do
        client.config["testing"] = "foo"
        client.config["testing"].should eq("foo")
        client.config.all["testing"].should eq("foo")
        client.config.clear("testing")
        client.config["testing"].should eq(nil)
      end
    end
    
    describe "#put" do
      it "can put, get, delete a job" do
        # In this test, I want to make sure that I can put a job into
        # a queue, and then retrieve its data
        #   1) put in a job
        #   2) get job
        #   3) delete job
        jid = q.put(Qless::Job, {"test" => "put_get"})
        job = client.job(jid)
        job.priority.should        eq(0)
        job.data.should            eq({"test" => "put_get"})
        job.tags.should            eq([])
        job.worker.should          eq("")
        job.state.should           eq("waiting")
        job.history.length.should  eq(1)
        job.history[0]['q'].should eq("testing")
      end
      
      it "can put, peek, and pop many" do
        # In this test, we're going to add several jobs, and make
        # sure that they:
        #   1) get put onto the queue
        #   2) we can peek at them
        #   3) we can pop them all off
        #   4) once we've popped them off, we can't get more
        jids = 10.times.collect { |x| q.put(Qless::Job, {"test" => "push_pop_many", "count" => x}) }
        jids.length.should eq(10)
        # Make sure peeks are non-destructive
        q.peek(7 ).length.should eq(7)
        q.peek(10).length.should eq(10)
        # Now let's pop them all off, destructively
        q.pop(7 ).length.should eq(7)
        q.pop(10).length.should eq(3)
      end
      
      it "can get all the attributes it expects after popping" do
        # In this test, we want to put a job, pop a job, and make
        # sure that when popped, we get all the attributes back 
        # that we expect
        #   1) put a job
        #   2) pop said job, check existence of attributes
        jid = q.put(Qless::Job, {'test' => 'test_put_pop_attributes'})
        client.config['heartbeat'] = 60
        job = q.pop
        job.data.should      eq({'test' => 'test_put_pop_attributes'})
        job.worker.should    eq(client.worker)
        job.expires.should   > (Time.new.to_i - 20)
        job.state.should     eq('running')
        job.queue.should     eq('testing')
        job.remaining.should eq(5)
        job.retries.should   eq(5)
        job.jid.should       eq(jid)
        job.klass.should     eq('Qless::Job')
        job.tags.should      eq([])
        jid = q.put(Qless::FooJob, 'test' => 'test_put_pop_attributes')
        job = q.pop
        job.klass.should include('FooJob')
      end

      it "can get all the attributes it expects after peeking" do
        # In this test, we want to put a job, peek a job, and make
        # sure that when peeks, we get all the attributes back 
        # that we expect
        #   1) put a job
        #   2) peek said job, check existence of attributes
        jid = q.put(Qless::Job, {'test' => 'test_put_pop_attributes'})
        job = q.peek
        job.data.should      eq({'test' => 'test_put_pop_attributes'})
        job.worker.should    eq('')
        job.state.should     eq('waiting')
        job.queue.should     eq('testing')
        job.remaining.should eq(5)
        job.retries.should   eq(5)
        job.jid.should       eq(jid)
        job.klass.should     eq('Qless::Job')
        job.tags.should      eq([])
        jid = q.put(Qless::FooJob, 'test' => 'test_put_pop_attributes')
        q.pop; job = q.peek
        job.klass.should include('FooJob')
      end
      
      it "can do data access as we expect" do
        # In this test, we'd like to make sure that all the data attributes
        # of the job can be accessed through __getitem__
        #   1) Insert a job
        #   2) Get a job,  check job['test']
        #   3) Peek a job, check job['test']
        #   4) Pop a job,  check job['test']
        job = client.job(q.put(Qless::Job, {"test" => "data_access"}))
        job["test"].should eq("data_access")
        q.peek["test"].should eq("data_access")
        q.pop[ "test"].should eq("data_access")
      end
      
      it "can handle priority correctly" do
        # In this test, we're going to add several jobs and make
        # sure that we get them in an order based on priority
        #   1) Insert 10 jobs into the queue with successively more priority
        #   2) Pop all the jobs, and ensure that with each pop we get the right one
        jids = 10.times.collect { |x| q.put(Qless::Job, {"test" => "put_pop_priority", "count" => x}, :priority => -x)}
        last = jids.length
        jids.length.times do |x|
          job = q.pop
          job["count"].should eq(last - 1)
          last = job["count"]
        end
      end
      
      it "maintains order for jobs with same priority" do
        # In this test, we want to make sure that jobs are popped
        # off in the same order they were put on, priorities being
        # equal.
        #   1) Put some jobs
        #   2) Pop some jobs, save jids
        #   3) Put more jobs
        #   4) Pop until empty, saving jids
        #   5) Ensure popped jobs are in the same order
        jids   = 20.times.collect { |x| q.put(Qless::Job, {"test" => "same priority order"})}
        popped = 10.times.collect { |x| q.pop.jid }
        10.times do
          jids   += 10.times.collect { |x| q.put(Qless::Job, {"test" => "same priority order"}) }
          popped +=  5.times.collect { |x| q.pop.jid }
        end
        popped += (q.length / 2).times.collect { |x| q.pop.jid }
        jids.should =~ popped
      end
      
      it "maintains a complete record of its history" do
        # In this test, we want to put a job, pop it, and then 
        # verify that its history has been updated accordingly.
        #   1) Put a job on the queue
        #   2) Get job, check history
        #   3) Pop job, check history
        #   4) Complete job, check history
        jid = q.put(Qless::Job, {"test" => "put_history"})
        job = client.job(jid)
        (job.history[0]["put"] - Time.now.to_i).abs.should < 1
        job = q.pop
        job = client.job(jid)
        (job.history[0]["popped"] - Time.now.to_i).abs.should < 1
      end
      
      it "peeks and pops empty queues with nil" do
        # Make sure that we can safely pop from an empty queue
        #   1) Make sure the queue is empty
        #   2) When we pop from it, we don't get anything back
        #   3) When we peek, we don't get anything
        q.length.should eq(0)
        q.pop( ).should eq(nil)
        q.peek.should eq(nil)
      end
    end  
    
    describe "#move" do
      it "can move jobs between queues" do
        # In this test, we want to verify that if we put a job
        # in one queue, and then move it, that it is in fact
        # no longer in the first queue.
        #   1) Put a job in one queue
        #   2) Put the same job in another queue
        #   3) Make sure that it's no longer in the first queue
        job = client.job(q.put(Qless::Job, {"test" => "move_queues"}))
        q.length.should     eq(1)
        other.length.should eq(0)
        job.move("other")
        q.length.should     eq(0)
        other.length.should eq(1)
      end
      
      it "expires locks when moved" do
        # In this test, we want to verify that if we put a job
        # in one queue, it's popped, and then we move it before
        # it's turned in, then subsequent attempts to renew the
        # lock or complete the work will fail
        #   1) Put job in one queue
        #   2) Pop that job
        #   3) Put job in another queue
        #   4) Verify that heartbeats fail
        jid = q.put(Qless::Job, {"test" => "move_queue_popped"})
        q.length.should eq(1)
        job = q.pop
        job.move("other")
        job.heartbeat.should eq(false)
      end
      
      it "moves non-destructively" do
        # In this test, we want to verify that if we move a job
        # from one queue to another, that it doesn't destroy any
        # of the other data that was associated with it. Like 
        # the priority, tags, etc.
        #   1) Put a job in a queue
        #   2) Get the data about that job before moving it
        #   3) Move it 
        #   4) Get the data about the job after
        #   5) Compare 2 and 4  
        jid = q.put(Qless::Job, {"test" => "move_non_destructive"}, :tags => ["foo", "bar"], :priority => 5)
        before = client.job(jid)
        before.move("other")
        after  = client.job(jid)
        before.tags.should     eq(["foo", "bar"])
        before.priority.should eq(5)
        before.tags.should     eq(after.tags)
        before.data.should     eq(after.data)
        before.priority.should eq(after.priority)
        after.history.length.should eq(2)
      end
    end
    
    describe "#heartbeat" do
      it "heartbeats as expected" do
        # In this test, we want to make sure that we can still 
        # keep our lock on an object if we renew it in time.
        # The gist of this test is:
        #   1) A gets an item, with positive heartbeat
        #   2) B tries to get an item, fails
        #   3) A renews its heartbeat successfully
        jid  = q.put(Qless::Job, {"test" => "heartbeat"})
        ajob = a.pop
        # Shouldn't get the job
        b.pop.should eq(nil)
        # It's renewed heartbeat should be a float in the future
        ajob.heartbeat.class.should eq(Fixnum)
        ajob.heartbeat.should >= Time.now.to_i
      end
      
      it "only allows jobs to be heartbeated if popped" do
        # In this test, we want to make sure that we cannot heartbeat
        # a job that has not yet been popped
        #   1) Put a job
        #   2) DO NOT pop that job
        #   3) Ensure we cannot heartbeat that job
        jid = q.put(Qless::Job, {"test" => "heartbeat_state"})
        client.job(jid).heartbeat.should eq(false)
      end
    end
    
    describe "#fail" do
      it "can put a job that's failed" do
        # In this test, we want to make sure that if we put a job
        # that has been failed, we want to make sure that it is
        # no longer reported as failed
        #   1) Put a job
        #   2) Fail that job
        #   3) Make sure we get failed stats
        #   4) Put that job on again
        #   5) Make sure that we no longer get failed stats
        job = client.job(q.put(Qless::Job, {"test" => "put_failed"}))
        job.fail("foo", "some message")
        client.failed.should eq({"foo" => 1})
        job.move("testing")
        q.length.should eq(1)
        client.failed.should eq({})
      end
      
      it "fails jobs correctly" do
        # In this test, we want to make sure that we can correctly 
        # fail a job
        #   1) Put a job
        #   2) Fail a job
        #   3) Ensure the queue is empty, and that there's something
        #       in the failed endpoint
        #   4) Ensure that the job still has its original queue
        client.failed.length.should eq(0)
        jid = q.put(Qless::Job, {"test" => "fail_failed"})
        job = client.job(jid)
        job.fail("foo", "some message")
        q.pop.should       eq(nil)
        client.failed.should eq({"foo" => 1})
        results = client.failed("foo")
        results["total"].should         eq(1)
        job = results["jobs"][0]
        job.jid.should       eq(jid)
        job.queue.should     eq("testing")
        job.data.should      eq({"test" => "fail_failed"})
        job.worker.should    eq("")
        job.state.should     eq("failed")
        job.queue.should     eq("testing")
        job.remaining.should eq(5)
        job.retries.should   eq(5)
        job.klass.should     eq('Qless::Job')
        job.tags.should      eq([])        
      end
      
      it "keeps us from completing jobs that we've failed" do
        # In this test, we want to make sure that we can pop a job,
        # fail it, and then we shouldn't be able to complete /or/ 
        # heartbeat the job
        #   1) Put a job
        #   2) Fail a job
        #   3) Heartbeat to job fails
        #   4) Complete job fails
        client.failed.length.should eq(0)
        jid = q.put(Qless::Job, {"test" => "pop_fail"})
        job = q.pop
        job.fail("foo", "some message")
        q.length.should eq(0)
        job.heartbeat.should eq(false)
        job.complete.should eq(false)
        client.failed.should eq({"foo" => 1})
        results = client.failed("foo")
        results["total"].should      eq(1)
        results["jobs"][0].jid.should eq(jid)
      end
      
      it "keeps us from failing a job that's already completed" do
        # Make sure that if we complete a job, we cannot fail it.
        #   1) Put a job
        #   2) Pop a job
        #   3) Complete said job
        #   4) Attempt to fail job fails
        client.failed.length.should eq(0)
        jid = q.put(Qless::Job, {"test" => "fail_complete"})
        job = q.pop
        job.complete.should eq('complete')
        client.job(jid).state.should eq('complete')
        job.fail("foo", "some message").should eq(false)
        client.failed.length.should eq(0)
      end
    end
    
    describe "#locks" do
      it "invalidates locks after they expire" do
        # In this test, we're going to have two queues that point
        # to the same queue, but we're going to have them represent
        # different workers. The gist of it is this
        #   1) A gets an item, with negative heartbeat
        #   2) B gets the same item,
        #   3) A tries to renew lock on item, should fail
        #   4) B tries to renew lock on item, should succeed
        #   5) Both clean up
        jid = q.put(Qless::Job, {"test" => "locks"})
        # Reset our heartbeat for both A and B
        client.config["heartbeat"] = -10
        # Make sure a gets a job
        ajob = a.pop
        bjob = b.pop
        ajob.jid.should eq(bjob.jid)
        bjob.heartbeat.class.should eq(Fixnum)
        (bjob.heartbeat + 11).should > Time.now.to_i
        ajob.heartbeat.should eq(false)
      end
    end
    
    describe "#cancel" do
      it "can cancel a job in the most basic way" do
        # In this test, we want to make sure that we can corretly
        # cancel a job
        #   1) Put a job
        #   2) Cancel a job
        #   3) Ensure that it's no longer in the queue
        #   4) Ensure that we can't get data for it
        jid = q.put(Qless::Job, {"test" => "cancel"})
        job = client.job(jid)
        q.length.should eq(1)
        job.cancel
        q.length.should eq(0)
        client.job(jid).should eq(nil)
      end
      
      it "can cancel a job, and prevent heartbeats" do
        # In this test, we want to make sure that when we cancel
        # a job, that heartbeats fail, as do completion attempts
        #   1) Put a job
        #   2) Pop that job
        #   3) Cancel that job
        #   4) Ensure that it's no longer in the queue
        #   5) Heartbeats fail, Complete fails
        #   6) Ensure that we can't get data for it
        jid = q.put(Qless::Job, {"test" => "cancel_heartbeat"})
        job = q.pop
        job.cancel
        q.length.should eq(0)
        job.heartbeat.should eq(false)
        job.complete.should eq(false)
        client.job( jid).should eq(nil)
      end
      
      it "can cancel a failed job" do
        # In this test, we want to make sure that if we fail a job
        # and then we cancel it, then we want to make sure that when
        # we ask for what jobs failed, we shouldn't see this one
        #   1) Put a job
        #   2) Fail that job
        #   3) Make sure we see failure stats
        #   4) Cancel that job
        #   5) Make sure that we don't see failure stats
        jid = q.put(Qless::Job, {"test" => "cancel_fail"})
        job = client.job(jid)
        job.fail("foo", "some message")
        client.failed.should eq({"foo" => 1})
        job.cancel
        client.failed.should eq({})
      end
    end
    
    describe "#complete" do
      it "can complete a job in the most basic way" do
        # In this test, we want to make sure that a job that has been
        # completed and not simultaneously enqueued are correctly 
        # marked as completed. It should have a complete history, and
        # have the correct state, no worker, and no queue
        #   1) Put an item in a queue
        #   2) Pop said item from the queue
        #   3) Complete that job
        #   4) Get the data on that job, check state
        jid = q.put(Qless::Job, {"test" => "complete"})
        job = q.pop
        job.complete.should eq("complete")
        job = client.job(jid)
        job.history.length.should eq(1)
        job.state.should  eq("complete")
        job.worker.should eq("")
        job.queue.should  eq("")
        q.length.should  eq(0)
      end
      
      it "can complete a job and immediately enqueue it" do
        # In this test, we want to make sure that a job that has been
        # completed and simultaneously enqueued has the correct markings.
        # It shouldn't have a worker, its history should be updated,
        # and the next-named queue should have that item.
        #   1) Put an item in a queue
        #   2) Pop said item from the queue
        #   3) Complete that job, re-enqueueing it
        #   4) Get the data on that job, check state
        #   5) Ensure that there is a work item in that queue
        jid = q.put(Qless::Job, {"test" => "complete_advance"})
        job = q.pop
        job.complete("testing").should eq("waiting")
        job = client.job(jid)
        job.history.length.should eq(2)
        job.state.should  eq("waiting")
        job.worker.should eq("")
        job.queue.should  eq("testing")
        q.length.should  eq(1)
      end
      
      it "can allows a job to be completed only by the current worker" do
        # In this test, we want to make sure that a job that has been
        # handed out to a second worker can both be completed by the
        # second worker, and not completed by the first.
        #   1) Hand a job out to one worker, expire
        #   2) Hand a job out to a second worker
        #   3) First worker tries to complete it, should fail
        #   4) Second worker tries to complete it, should succeed
        jid = q.put(Qless::Job, {"test" => "complete_fail"})
        client.config["heartbeat"] = -10
        ajob = a.pop
        ajob.jid.should eq(jid)
        bjob = b.pop
        bjob.jid.should eq(jid)
        ajob.complete.should eq(false)
        bjob.complete.should eq("complete")
        job = client.job(jid)
        job.history.length.should eq(1)
        job.state.should  eq("complete")
        job.worker.should eq("")
        job.queue.should  eq("")
        q.length.should  eq(0)
      end
      
      it "can allow only popped jobs to be completed" do
        # In this test, we want to make sure that if we try to complete
        # a job that's in anything but the 'running' state.
        #   1) Put an item in a queue
        #   2) DO NOT pop that item from the queue
        #   3) Attempt to complete the job, ensure it fails
        jid = q.put(Qless::Job, "test" => "complete_fail")
        job = client.job(jid)
        job.complete("testing").should eq(false)
        job.complete.should eq(false)
      end
      
      it "can ensure that the next queue appears in the queues endpoint" do
        # In this test, we want to make sure that if we complete a job and
        # advance it, that the new queue always shows up in the 'queues'
        # endpoint.
        #   1) Put an item in a queue
        #   2) Complete it, advancing it to a different queue
        #   3) Ensure it appears in 'queues'
        jid = q.put(Qless::Job, {"test" => "complete_queues"})
        client.queues.select { |q| q["name"] == "other" }.length.should eq(0)
        q.pop.complete("other").should eq("waiting")
        client.queues.select { |q| q["name"] == "other" }.length.should eq(1)
      end
    end
    
    describe "#schedule" do
      it "can scheduled a job" do
        # In this test, we'd like to make sure that we can't pop
        # off a job scheduled for in the future until it has been
        # considered valid
        #   1) Put a job scheduled for 10s from now
        #   2) Ensure an empty pop
        #   3) 'Wait' 10s
        #   4) Ensure pop contains that job
        # This is /ugly/, but we're going to path the time function so
        # that we can fake out how long these things are waiting
        start = Time.now
        Time.stub!(:now).and_return(start)
        jid = q.put(Qless::Job, {"test" => "scheduled"}, :delay => 10)
        q.pop.should    eq(nil)
        q.length.should eq(1)
        Time.stub!(:now).and_return(start + 11)
        q.pop.jid.should eq(jid)
      end
    end
    
    describe "#expiration" do
      it "can expire job data subject to a job expiration policy" do
        # In this test, we want to make sure that we honor our job
        # expiration, in the sense that when jobs are completed, we 
        # then delete all the jobs that should be expired according
        # to our deletion criteria
        #   1) First, set jobs-history to -1
        #   2) Then, insert a bunch of jobs
        #   3) Pop each of these jobs
        #   4) Complete each of these jobs
        #   5) Ensure that we have no data about jobs
        client.config["jobs-history"] = -1
        jids = 20.times.collect { |x| q.put(Qless::Job, {"test" => "job_time_expiration", "count" => x}) }
        jids.each { |jid| q.pop.complete }
        @redis.zcard("ql:completed").should eq(0)
        @redis.keys("ql:j:*").length.should eq(0)
      end
      
      it "can expire job data based on time" do
        # In this test, we want to make sure that we honor our job
        # expiration, in the sense that when jobs are completed, we 
        # then delete all the jobs that should be expired according
        # to our deletion criteria
        #   1) First, set jobs-history-count to 10
        #   2) Then, insert 20 jobs
        #   3) Pop each of these jobs
        #   4) Complete each of these jobs
        #   5) Ensure that we have data about 10 jobs
        client.config["jobs-history-count"] = 10
        jids = 20.times.collect { |x| q.put(Qless::Job, {"test" => "job_time_expiration", "count" => x}) }
        jids.each { |jid| q.pop.complete }
        @redis.zcard("ql:completed").should eq(10)
        @redis.keys("ql:j:*").length.should eq(10)        
      end
    end
    
    describe "#stats" do
      it "can correctly track wait times" do
        # In this test, we're going to make sure that statistics are
        # correctly collected about how long items wait in a queue
        #   1) Ensure there are no wait stats currently
        #   2) Add a bunch of jobs to a queue
        #   3) Pop a bunch of jobs from that queue, faking out the times
        #   4) Ensure that there are now correct wait stats
        stats = q.stats(Time.now.to_i)
        stats["wait"]["count"].should eq(0)
        stats["run" ]["count"].should eq(0)
        # This is /ugly/, but we're going to path the time function so
        # that we can fake out how long these things are waiting
        start = Time.now
        Time.stub!(:now).and_return(start)
        jids = 20.times.collect { |x| q.put(Qless::Job, {"test" => "stats_waiting", "count" => x}) }
        jids.length.should eq(20)
        jids.length.times.each do |c|
          Time.stub!(:now).and_return(start + c)
          job = q.pop
        end
        
        stats = q.stats(start.to_i)
        stats["wait"]["count"].should eq(20)
        stats["wait"]["mean" ].should eq(9.5)
        (stats["wait"]["std" ] - 5.916079783099).should < 1e-8
        stats["wait"]["histogram"][0...20].should eq(20.times.map { |x| 1 })
        stats["run" ]["histogram"].reduce(0, :+).should eq(stats["run" ]["count"])
        stats["wait"]["histogram"].reduce(0, :+).should eq(stats["wait"]["count"])
      end
      
      it "can correctly track completion times" do
        # In this test, we want to make sure that statistics are
        # correctly collected about how long items take to actually 
        # get processed.
        #   1) Ensure there are no run stats currently
        #   2) Add a bunch of jobs to a queue
        #   3) Pop those jobs
        #   4) Complete those jobs, faking out the time
        #   5) Ensure that there are now correct run stats
        stats = q.stats(Time.now.to_i)
        stats["wait"]["count"].should eq(0)
        stats["run" ]["count"].should eq(0)
        # This is /ugly/, but we're going to path the time function so
        # that we can fake out how long these things are waiting
        start = Time.now
        Time.stub!(:now).and_return(start)
        jids = 20.times.collect { |x| q.put(Qless::Job, {"test" => "stats_waiting", "count" => x}) }
        jids.length.should eq(20)
        jobs = q.pop(20)
        jids.length.times.each do |c|
          Time.stub!(:now).and_return(start + c)
          jobs[c].complete
        end
        
        stats = q.stats(start.to_i)
        stats["run"]["count"].should eq(20)
        stats["run"]["mean" ].should eq(9.5)
        (stats["run"]["std" ] - 5.916079783099).should < 1e-8
        stats["run" ]["histogram"][0...20].should eq(20.times.map { |x| 1 })
        stats["run" ]["histogram"].reduce(0, :+).should eq(stats["run"]["count"])
        stats["wait"]["histogram"].reduce(0, :+).should eq(stats["run"]["count"])
      end
      
      it "can track failed jobs" do
        # In this test, we want to make sure that statistics are
        # correctly collected about how many items are currently failed
        #   1) Put an item
        #   2) Ensure we don't have any failed items in the stats for that queue
        #   3) Fail that item
        #   4) Ensure that failures and failed both increment
        #   5) Put that item back
        #   6) Ensure failed decremented, failures untouched
        jid = q.put(Qless::Job, {"test" => "stats_failed"})
        q.stats["failed"  ].should eq(0)
        q.stats["failures"].should eq(0)
        client.job(jid).fail("foo", "bar")
        q.stats["failed"  ].should eq(1)
        q.stats["failures"].should eq(1)
        client.job(jid).move("testing")
        q.stats["failed"  ].should eq(0)
        q.stats["failures"].should eq(1)
      end
      
      it "can correctly track retries" do
        # In this test, we want to make sure that retries are getting
        # captured correctly in statistics
        #   1) Put a job
        #   2) Pop job, lose lock
        #   3) Ensure no retries in stats
        #   4) Pop job,
        #   5) Ensure one retry in stats
        jid = q.put(Qless::Job, {"test" => "stats_retries"})
        client.config["heartbeat"] = -10
        q.pop; q.stats["retries"].should eq(0)
        q.pop; q.stats["retries"].should eq(1)
      end
      
      it "can update stats for the original day with respect to failures" do
        # In this test, we want to verify that if we unfail a job on a
        # day other than the one on which it originally failed, that we
        # the `failed` stats for the original day are decremented, not
        # today.
        #   1) Put a job
        #   2) Fail that job
        #   3) Advance the clock 24 hours
        #   4) Put the job back
        #   5) Check the stats with today, check failed = 0, failures = 0
        #   6) Check 'yesterdays' stats, check failed = 0, failures = 1
        job = client.job(q.put(Qless::Job, {"test" => "stats_failed_original_day"}))
        job.fail("foo", "bar")
        stats = q.stats
        stats["failures"].should eq(1)
        stats["failed"  ].should eq(1)
        start = Time.now
        Time.stub!(:now).and_return(start + 86400)
        job.move("testing")
        today = q.stats
        today["failures"].should eq(0)
        today["failed"  ].should eq(0)
        Time.stub!(:now).and_return(start)
        yesterday = q.stats
        yesterday["failures"].should eq(1)
        yesterday["failed"  ].should eq(0)
      end
    end
    
    describe "#queues" do
      it "can correctly use the 'queues' endpoint" do
        # In this test, we want to make sure that the queues function
        # can correctly identify the numbers associated with that queue
        #   1) Make sure we get nothing for no queues
        #   2) Put delayed item, check
        #   3) Put item, check
        #   4) Put, pop item, check
        #   5) Put, pop, lost item, check
        client.queues.should eq({})
        q.put(Qless::Job, {"test" => "queues"}, :delay => 10)
        expected = {
          "name"      => "testing",
          "stalled"   => 0,
          "waiting"   => 0,
          "running"   => 0,
          "scheduled" => 1
        }
        client.queues.should eq([expected])
        client.queues("testing").should eq(expected)
        
        q.put(Qless::Job, {"test" => "queues"})
        expected["waiting"] += 1
        client.queues.should eq([expected])
        client.queues("testing").should eq(expected)
        
        job = q.pop
        expected["waiting"] -= 1
        expected["running"] += 1
        client.queues.should eq([expected])
        client.queues("testing").should eq(expected)
        
        q.put(Qless::Job, {"test" => "queues"})
        client.config["heartbeat"] = -10
        job = q.pop
        expected["stalled"] += 1
        client.queues.should eq([expected])
        client.queues("testing").should eq(expected)
      end
    end
    
    describe "#track" do
      it "can start tracking a job" do
        # In this test, we want to make sure that tracking works as expected.
        #   1) Check tracked jobs, expect none
        #   2) Put, Track a job, check
        #   3) Untrack job, check
        #   4) Track job, cancel, check
        client.tracked.should eq({"expired" => {}, "jobs" => []})
        job = client.job(q.put(Qless::Job, {"test" => "track"}))
        job.track
        client.tracked["jobs"].length.should eq(1)
        job.untrack
        client.tracked["jobs"].length.should eq(0)
        job.track
        job.cancel
        client.tracked["expired"].should eq([job.jid])
      end
      
      it "knows when a job is tracked" do
        # When peeked, popped, failed, etc., qless should know when a 
        # job is tracked or not
        # => 1) Put a job, track it
        # => 2) Peek, ensure tracked
        # => 3) Pop, ensure tracked
        # => 4) Fail, check failed, ensure tracked
        job = client.job(q.put(Qless::Job, {"test" => "track_tracked"}))
        job.track
        q.peek.tracked.should eq(true)
        job = q.pop
        job.tracked.should eq(true)
        job.fail("foo", "bar")
        client.failed("foo")["jobs"][0].tracked.should eq(true)
      end
      
      it "knows when a job is not tracked" do
        # When peeked, popped, failed, etc., qless should know when a 
        # job is not tracked
        # => 1) Put a job
        # => 2) Peek, ensure tracked
        # => 3) Pop, ensure tracked
        # => 4) Fail, check failed, ensure tracked
        job = client.job(q.put(Qless::Job, {"test" => "track_tracked"}))
        q.peek.tracked.should eq(false)
        job = q.pop
        job.tracked.should eq(false)
        job.fail("foo", "bar")
        client.failed("foo")["jobs"][0].tracked.should eq(false)
      end
      
      it "can also save a tag when tracking a job" do
        # In this test, we want to make sure that when we begin tracking
        # a job, we can optionally provide tags with it, and those tags
        # get saved.
        #   1) Put job, ensure no tags
        #   2) Track job, ensure tags
        job = client.job(q.put(Qless::Job, {"test" => "track_tag"}))
        job.tags.should eq([])
        job.track("foo", "bar")
        client.job(job.jid).tags.should eq(["foo", "bar"])
      end
    end
    
    describe "#retries" do
      it "can keep track of the appropriate number of retries for a job" do
        # In this test, we want to make sure that jobs are given a
        # certain number of retries before automatically being considered
        # failed.
        #   1) Put a job with a few retries
        #   2) Verify there are no failures
        #   3) Lose the heartbeat as many times
        #   4) Verify there are failures
        #   5) Verify the queue is empty
        client.failed.should eq({})
        q.put(Qless::Job, {"test" => "retries"}, :retries => 2)
        client.config["heartbeat"] = -10
        q.pop; client.failed.should eq({})
        q.pop; client.failed.should eq({})
        q.pop; client.failed.should eq({})
        q.pop; client.failed.should eq({
          "failed-retries-testing" => 1
        })
      end
      
      it "can reset the number of remaining retries when completed and put into a new queue" do
        # In this test, we want to make sure that jobs have their number
        # of remaining retries reset when they are put on a new queue
        #   1) Put an item with 2 retries
        #   2) Lose the heartbeat once
        #   3) Get the job, make sure it has 1 remaining
        #   4) Complete the job
        #   5) Get job, make sure it has 2 remaining
        q.put(Qless::Job, {"test" => "retries_complete"}, :retries => 2)
        client.config["heartbeat"] = -10
        job = q.pop; job = q.pop
        job.remaining.should eq(1)
        job.complete
        client.job(job.jid).remaining.should eq(2)
      end
      
      it "can reset the number of remaining retries when put in a new queue" do
        # In this test, we want to make sure that jobs have their number
        # of remaining retries reset when they are put on a new queue
        #   1) Put an item with 2 retries
        #   2) Lose the heartbeat once
        #   3) Get the job, make sure it has 1 remaining
        #   4) Re-put the job in the queue with job.move
        #   5) Get job, make sure it has 2 remaining
        q.put(Qless::Job, {"test" => "retries_put"}, :retries => 2)
        client.config["heartbeat"] = -10
        job = q.pop; job = q.pop
        job.retries.should eq(2)
        job.remaining.should eq(1)
        job.move("testing")
        client.job(job.jid).remaining.should eq(2)
      end
    end
    
    describe "#workers" do
      it "can handle worker stats in the most basic case" do
        # In this test, we want to verify that when we add a job, we 
        # then know about that worker, and that it correctly identifies
        # the jobs it has.
        #   1) Put a job
        #   2) Ensure empty 'workers'
        #   3) Pop that job
        #   4) Ensure unempty 'workers'
        #   5) Ensure unempty 'worker'
        jid = q.put(Qless::Job, {"test" => "workers"})
        client.workers.should eq({})
        job = q.pop
        client.workers.should eq([{
          "name"    => q.worker,
          "jobs"    => 1,
          "stalled" => 0
        }])
        client.workers(q.worker).should eq({
          "jobs"    => [jid],
          "stalled" => {}
        })
      end
      
      it "can remove a job from a worker's stats when it's canceled" do
        # In this test, we want to verify that when a job is canceled,
        # that it is removed from the list of jobs associated with a worker
        #   1) Put a job
        #   2) Pop that job
        #   3) Ensure 'workers' and 'worker' know about it
        #   4) Cancel job
        #   5) Ensure 'workers' and 'worker' reflect that
        jid = q.put(Qless::Job, {"test" => "workers_cancel"})
        job = q.pop
        client.workers.should eq([{
          "name"    => q.worker,
          "jobs"    => 1,
          "stalled" => 0
        }])
        client.workers(q.worker).should eq({
          "jobs"    => [jid],
          "stalled" => {}
        })
        job.cancel
        client.workers.should eq([{
          "name"    => q.worker,
          "jobs"    => 0,
          "stalled" => 0
        }])
        client.workers(q.worker).should eq({
          "jobs"    => {},
          "stalled" => {}
        })
      end
      
      it "can make note of the number of stalled jobs on a worker" do
        # In this test, we want to verify that 'workers' and 'worker'
        # correctly identify that a job is stalled, and that when that
        # job is taken from the lost lock, that it's no longer listed
        # as stalled under the original worker. Also, that workers are
        # listed in order of recency of contact
        #   1) Put a job
        #   2) Pop a job, with negative heartbeat
        #   3) Ensure 'workers' and 'worker' show it as stalled
        #   4) Pop the job with a different worker
        #   5) Ensure 'workers' and 'worker' reflect that
        jid = q.put(Qless::Job, {"test" => "workers_lost_lock"})
        client.config["heartbeat"] = -10
        job = q.pop
        client.workers.should eq([{
          "name"    => q.worker,
          "jobs"    => 0,
          "stalled" => 1
        }])
        client.workers(q.worker).should eq({
          "jobs"    => {},
          "stalled" => [jid]
        })
        
        client.config["heartbeat"] = 60
        job = a.pop
        a.pop
        client.workers.should eq([{
          "name"    => a.worker,
          "jobs"    => 1,
          "stalled" => 0
        }, {
          "name"    => q.worker,
          "jobs"    => 0,
          "stalled" => 0
        }])
        client.workers(q.worker).should eq({
          "jobs"    => {},
          "stalled" => {}
        })
      end
      
      it "can remove a job from a worker's list of jobs when failed" do
        # In this test, we want to make sure that when we fail a job,
        # its reflected correctly in 'workers' and 'worker'
        #   1) Put a job
        #   2) Pop job, check 'workers', 'worker'
        #   3) Fail that job
        #   4) Check 'workers', 'worker'
        jid = q.put(Qless::Job, {"test" => "workers_fail"})
        job = q.pop
        client.workers.should eq([{
          "name"    => q.worker,
          "jobs"    => 1,
          "stalled" => 0
        }])
        client.workers(q.worker).should eq({
          "jobs"    => [jid],
          "stalled" => {}
        })
        
        # Now, let's fail it
        job.fail("foo", "bar")
        client.workers.should eq([{
          "name"    => q.worker,
          "jobs"    => 0,
          "stalled" => 0
        }])
        client.workers(q.worker).should eq({
          "jobs"    => {},
          "stalled" => {}
        })
      end
      
      it "can remove completed jobs from a worker's stats" do
        # In this test, we want to make sure that when we complete a job,
        # it's reflected correctly in 'workers' and 'worker'
        #   1) Put a job
        #   2) Pop a job, check 'workers', 'worker'
        #   3) Complete job, check 'workers', 'worker'
        jid = q.put(Qless::Job, {"test" => "workers_complete"})
        job = q.pop
        client.workers.should eq([{
          "name"    => q.worker,
          "jobs"    => 1,
          "stalled" => 0
        }])
        client.workers(q.worker).should eq({
          "jobs"    => [jid],
          "stalled" => {}
        })
        
        job.complete
        client.workers.should eq([{
          "name"    => q.worker,
          "jobs"    => 0,
          "stalled" => 0
        }])
        client.workers(q.worker).should eq({
          "jobs"    => {},
          "stalled" => {}
        })
      end
      
      it "removes jobs from a worker's info when it's put on another queue" do
        # Make sure that if we move a job from one queue to another, that 
        # the job is no longer listed as one of the jobs that the worker
        # has.
        #   1) Put a job
        #   2) Pop job, check 'workers', 'worker'
        #   3) Move job, check 'workers', 'worker'
        jid = q.put(Qless::Job, {"test" => "workers_reput"})
        job = q.pop
        client.workers.should eq([{
          "name"    => q.worker,
          "jobs"    => 1,
          "stalled" => 0
        }])
        client.workers(q.worker).should eq({
          "jobs"    => [jid],
          "stalled" => {}
        })
        
        job.move("other")
        client.workers.should eq([{
          "name"    => q.worker,
          "jobs"    => 0,
          "stalled" => 0
        }])
        client.workers(q.worker).should eq({
          "jobs"    => {},
          "stalled" => {}
        })
      end
    end
    
    describe "#jobs" do
      it "" do
        # Make sure that we can get a list of jids for a queue that
        # are running, stalled and scheduled
        #   1) Put a job, pop it, check 'running'
        #   2) Put a job scheduled, check 'scheduled'
        #   3) Put a job with negative heartbeat, pop, check stalled
        jid = q.put(Qless::Job, {"test" => "rss"})
        job = q.pop
        q.running.should eq([jid])
        jid = q.put(Qless::Job, {"test" => "rss"}, :delay => 60)
        q.scheduled.should eq([jid])
        client.config["heartbeat"] = -60
        jid = q.put(Qless::Job, {"test" => "rss"})
        q.pop
        q.stalled.should eq([jid])
      end
    end
    
    describe "#lua" do
      it "checks cancel's arguments" do
        cancel = Qless::Lua.new("cancel", @redis)
        # Providing in keys
        lambda { cancel(["foo"], ["deadbeef"]) }.should raise_error
        # Missing an id
        lambda { cancel([], []) }.should raise_error
      end
      
      it "checks complete's arguments" do
        complete = Qless::Lua.new("complete", @redis)
        [
          # Not enough args
          [[], []],
          # Providing a key, but shouldn't
          [["foo"], ["deadbeef", "worker1", "foo", 12345]],
          # Missing worker
          [[], ["deadbeef"]],
          # Missing queue
          [[], ["deadbeef", "worker1"]],
          # Missing now
          [[], ["deadbeef", "worker1", "foo"]],
          # Malformed now
          [[], ["deadbeef", "worker1", "foo", "howdy"]],
          # Malformed JSON
          [[], ["deadbeef", "worker1", "foo", 12345, "[}"]],
          # Not a number for delay
          [[], ['deadbeef', 'worker1', 'foo', 12345, '{}', 'foo', 'howdy']]
        ].each { |x| lambda { complete(x[0], x[1]) }.should raise_error }        
      end
      
      it "checks fail's arguments" do
        fail = Qless::Lua.new("fail", @redis)
        [
          # Passing in keys
          [["foo"], ["deadbeef", "worker1", "foo", "bar", 12345]],
          # Missing id
          [[], []],
          # Missing worker
          [[], ["deadbeef"]],
          # Missing type
          [[], ["deadbeef", "worker1"]],
          # Missing message
          [[], ["deadbeef", "worker1", "foo"]],
          # Missing now
          [[], ["deadbeef", "worker1", "foo", "bar"]],
          # Malformed now
          [[], ["deadbeef", "worker1", "foo", "bar", "howdy"]],
          # Malformed data
          [[], ["deadbeef", "worker1", "foo", "bar", 12345, "[}"]],
        ].each { |x| lambda { fail(x[0], x[1]) }.should raise_error }
      end
      
      it "checks failed's arguments" do
        failed = Qless::Lua.new("failed", @redis)
        [
          # Passing in keys
          [["foo"], []],
          # Malformed start
          [["foo"], ["bar", "howdy"]],
          # Malformed limit
          [["foo"], ["bar", 0, "howdy"]]
        ].each { |x| lambda { failed(x[0], x[1]) }.should raise_error }
      end
      
      it "checks get's arguments" do
        get = Qless::Lua.new("get", @redis)
        [
          # Passing in keys
          [["foo"], ["deadbeef"]],
          # Missing id
          [[], []]
        ].each { |x| lambda { get(x[0], x[1]) }.should raise_error }
      end
      
      it "checks getconfig's arguments" do
        getconfig = Qless::Lua.new("getconfig", @redis)
        [
          # Passing in keys
          [["foo"], []]
        ].each { |x| lambda { getconfig(x[0], x[1]) }.should raise_error }
      end
      
      it "checks heartbeat's arguments" do
        heartbeat = Qless::Lua.new("heartbeat", @redis)
        [
          # Passing in keys
          [["foo"], ["deadbeef", "foo", 12345]],
          # Missing id
          [[], []],
          # Missing worker
          [[], ["deadbeef"]],
          # Missing expiration
          [[], ["deadbeef", "worker1"]],
          # Malformed expiration
          [[], ["deadbeef", "worker1", "howdy"]],
          # Malformed JSON
          [[], ["deadbeef", "worker1", 12345, "[}"]]
        ].each { |x| lambda { heartbeat(x[0], x[1]) }.should raise_error }
      end
      
      it "checks jobs' arguments" do
        jobs = Qless::Lua.new('jobs', @redis)
        [
          # Providing keys
          [['foo'], []],
          # Unrecognized option
          [[], ['testing']],
          # Missing now
          [[], ['stalled']],
          # Malformed now
          [[], ['stalled', 'foo']],
          # Missing queue
          [[], ['stalled', 12345]]
        ]
      end
      
      it "checks peek's arguments" do
        peek = Qless::Lua.new("peek", @redis)
        [
          # Passing in no keys
          [[], [1, 12345]],
          # Passing in too many keys
          [["foo", "bar"], [1, 12345]],
          # Missing count
          [["foo"], []],
          # Malformed count
          [["foo"], ["howdy"]],
          # Missing now
          [["foo"], [1]],
          # Malformed now
          [["foo"], [1, "howdy"]]
        ].each { |x| lambda { peek(x[0], x[1]) }.should raise_error }
      end
      
      it "checks pop's arguments" do
        pop = Qless::Lua.new("pop", @redis)
        [
          # Passing in no keys
          [[], ["worker1", 1, 12345, 12346]],
          # Passing in too many keys
          [["foo", "bar"], ["worker1", 1, 12345, 12346]],
          # Missing worker
          [["foo"], []],
          # Missing count
          [["foo"], ["worker1"]],
          # Malformed count
          [["foo"], ["worker1", "howdy"]],
          # Missing now
          [["foo"], ["worker1", 1]],
          # Malformed now
          [["foo"], ["worker1", 1, "howdy"]],
          # Missing expires
          [["foo"], ["worker1", 1, 12345]],
          # Malformed expires
          [["foo"], ["worker1", 1, 12345, "howdy"]]
        ].each { |x| lambda { pop(x[0], x[1]) }.should raise_error }
      end
      
      it "checks put's arguments" do
        put = Qless::Lua.new("put", @redis)
        [
          # Passing in no keys
          [[], ["deadbeef", "{}", 12345]],
          # Passing in two keys
          [["foo", "bar"], ["deadbeef", "{}", 12345]],
          # Missing id
          [["foo"], []],
          # Missing data
          [["foo"], ["deadbeef"]],
          # Malformed data
          [["foo"], ["deadbeef", "[}"]],
          # Non-dictionary data
          [["foo"], ["deadbeef", "[]"]],
          # Non-dictionary data
          [["foo"], ["deadbeef", "\"foobar\""]],
          # Missing now
          [["foo"], ["deadbeef", "{}"]],
          # Malformed now
          [["foo"], ["deadbeef", "{}", "howdy"]],
          # Malformed priority
          [["foo"], ["deadbeef", "{}", 12345, "howdy"]],
          # Malformed tags
          [["foo"], ["deadbeef", "{}", 12345, 0, "[}"]],
          # Malformed dleay
          [["foo"], ["deadbeef", "{}", 12345, 0, "[]", "howdy"]]          
        ].each { |x| lambda { put(x[0], x[1]) }.should raise_error }
      end
      
      it "checks queues' arguments" do
        queues = Qless::Lua.new("queues", @redis)
        [
          # Passing in keys
          [["foo"], [12345]],
          # Missing time
          [[], []],
          # Malformed time
          [[], ["howdy"]]          
        ].each { |x| lambda { queues(x[0], x[1]) }.should raise_error }
      end
      
      it "checks setconfig's arguments" do
        setconfig = Qless::Lua.new("setconfig", @redis)
        [
          # Passing in keys
          [["foo"], []]          
        ].each { |x| lambda { setconfig(x[0], x[1]) }.should raise_error }
      end
      
      it "checks stats' arguments" do
        stats = Qless::Lua.new("stats", @redis)
        [
          # Passing in keys
          [["foo"], ["foo", "bar"]],
          # Missing queue
          [[], []],
          # Missing date
          [[], ["foo"]]          
        ].each { |x| lambda { stats(x[0], x[1]) }.should raise_error }
      end
      
      it "checks track's arguments" do
        track = Qless::Lua.new("track", @redis)
        [
          # Passing in keys
          [["foo"], []],
          # Unknown command
          [[], ["fslkdjf", "deadbeef", 12345]],
          # Missing jid
          [[], ["track"]],
          # Missing time
          [[], ["track", "deadbeef"]],
          # Malformed time
          [[], ["track", "deadbeef", "howdy"]]          
        ].each { |x| lambda { track(x[0], x[1]) }.should raise_error }
      end
    end
  end
end
