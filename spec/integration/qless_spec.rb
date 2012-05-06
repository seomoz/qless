require 'spec_helper'
require "qless"
require "redis"
require "json"
require 'yaml'

module Qless
  class FooJob
    # An empty class
  end
  
  describe Qless::Client, :integration do
    # Our main test queue
    let(:q) { client.queue("testing") }
    # Point to the main queue, but identify as different workers
    let(:a) { client.queue("testing").tap { |o| o.worker_name = "worker-a" } }
    let(:b) { client.queue("testing").tap { |o| o.worker_name = "worker-b" } }
    # And a second queue
    let(:other) { client.queue("other")   }
    
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
        job.worker_name.should          eq("")
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
        job.worker_name.should    eq(Qless.worker_name)
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
        job.worker_name.should    eq('')
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
        jids   = []
        popped = []
        200.times do |count|
          jids.push(q.put(Qless::Job, {"test" => "same priority order", "count" => 2 * count}))
          q.peek
          jids.push(q.put(Qless::FooJob, {"test" => "same priority order", "count" => 2 * count + 1 }))
          popped.push(q.pop.jid)
          q.peek
        end
        popped += 200.times.collect do |count|
          q.pop.jid
        end
        jids.should eq(popped)
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
        # Try setting a queue-specific heartbeat
        q.heartbeat = -60
        ajob.heartbeat.class.should eq(Fixnum)
        ajob.heartbeat.should <= Time.now.to_i
      end
      
      it "resets the job's expiration in the queue when heartbeated" do
        # In this test, we want to make sure that when we heartbeat a 
        # job, its expiration in the queue is also updated. So, supposing
        # that I heartbeat a job 5 times, then its expiration as far as
        # the lock itself is concerned is also updated
        client.config['crawl-heartbeat'] = 7200
        jid = q.put(Qless::Job, {})
        job = q.pop
        b.pop.should eq(nil)
        
        start = Time.now
        Time.stub!(:now).and_return(start)
        10.times do |i|
          Time.stub!(:now).and_return(start + i * 3600)
          job.heartbeat.should_not eq(false)
          b.pop.should eq(nil)
        end
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
        job = q.pop
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
        job = q.pop
        job.fail("foo", "some message")
        q.pop.should       eq(nil)
        client.failed.should eq({"foo" => 1})
        results = client.failed("foo")
        results["total"].should         eq(1)
        job = results["jobs"][0]
        job.jid.should       eq(jid)
        job.queue.should     eq("testing")
        job.data.should      eq({"test" => "fail_failed"})
        job.worker_name.should    eq("")
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
      
      it "erases failure data once a previously-failed job completes" do
        # No matter if a job has been failed before or not, then we
        # should delete the failure information we have once a job
        # has completed.
        jid = q.put(Qless::Job, {"test" => "complete_failed"})
        job = q.pop
        job.fail("foo", "some message")
        job.move("testing")
        job = q.pop
        job.complete.should eq("complete")
        client.job(jid).failure.should eq({})
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
        job = q.pop
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
        job.worker_name.should eq("")
        job.queue.should  eq("")
        q.length.should  eq(0)
        
        # If we put it into another queue, it shouldn't appear in the complete
        # endpoint anymore
        job.move("testing")
        client.complete.should eq([])
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
        job.worker_name.should eq("")
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
        job.worker_name.should eq("")
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
      
      it "can correctly represent the state of a scheduled job" do
        # Despite the wordy test name, we want to make sure that
        # when a job is put with a delay, that its state is 
        # 'scheduled', when we peek it or pop it and its state is
        # now considered valid, then it should be 'waiting'
        start = Time.now
        Time.stub!(:now).and_return(start)
        jid = q.put(Qless::Job, {"test" => "scheduled_state"}, :delay => 10)
        client.job(jid).state.should eq("scheduled")
        Time.stub!(:now).and_return(start + 11)
        q.peek.state.should eq("waiting")
        client.job(jid).state.should eq("waiting")
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
        q.pop.fail("foo", "bar")
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
        job = q.pop()
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
          "scheduled" => 1,
          "depends"   => 0
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
          "name"    => q.worker_name,
          "jobs"    => 1,
          "stalled" => 0
        }])
        client.workers(q.worker_name).should eq({
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
          "name"    => q.worker_name,
          "jobs"    => 1,
          "stalled" => 0
        }])
        client.workers(q.worker_name).should eq({
          "jobs"    => [jid],
          "stalled" => {}
        })
        job.cancel
        client.workers.should eq([{
          "name"    => q.worker_name,
          "jobs"    => 0,
          "stalled" => 0
        }])
        client.workers(q.worker_name).should eq({
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
          "name"    => q.worker_name,
          "jobs"    => 0,
          "stalled" => 1
        }])
        client.workers(q.worker_name).should eq({
          "jobs"    => {},
          "stalled" => [jid]
        })
        
        client.config["heartbeat"] = 60
        job = a.pop
        a.pop
        client.workers.should eq([{
          "name"    => a.worker_name,
          "jobs"    => 1,
          "stalled" => 0
        }, {
          "name"    => q.worker_name,
          "jobs"    => 0,
          "stalled" => 0
        }])
        client.workers(q.worker_name).should eq({
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
          "name"    => q.worker_name,
          "jobs"    => 1,
          "stalled" => 0
        }])
        client.workers(q.worker_name).should eq({
          "jobs"    => [jid],
          "stalled" => {}
        })
        
        # Now, let's fail it
        job.fail("foo", "bar")
        client.workers.should eq([{
          "name"    => q.worker_name,
          "jobs"    => 0,
          "stalled" => 0
        }])
        client.workers(q.worker_name).should eq({
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
          "name"    => q.worker_name,
          "jobs"    => 1,
          "stalled" => 0
        }])
        client.workers(q.worker_name).should eq({
          "jobs"    => [jid],
          "stalled" => {}
        })
        
        job.complete
        client.workers.should eq([{
          "name"    => q.worker_name,
          "jobs"    => 0,
          "stalled" => 0
        }])
        client.workers(q.worker_name).should eq({
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
          "name"    => q.worker_name,
          "jobs"    => 1,
          "stalled" => 0
        }])
        client.workers(q.worker_name).should eq({
          "jobs"    => [jid],
          "stalled" => {}
        })
        
        job.move("other")
        client.workers.should eq([{
          "name"    => q.worker_name,
          "jobs"    => 0,
          "stalled" => 0
        }])
        client.workers(q.worker_name).should eq({
          "jobs"    => {},
          "stalled" => {}
        })
      end
    end
    
    describe "#jobs" do
      it "lets us peek at jobs in various states in a queue" do
        # Make sure that we can get a list of jids for a queue that
        # are running, stalled and scheduled
        #   1) Put a job, pop it, check 'running'
        #   2) Put a job scheduled, check 'scheduled'
        #   3) Put a job with negative heartbeat, pop, check stalled
        #   4) Put a job dependent on another and check 'depends'
        jids = 20.times.map { |i| q.put(Qless::Job, {"test" => "rssd"})}.to_set
        require 'set'
        client.config["heartbeat"] = -60
        jobs = q.pop(20)
        (q.stalled(0, 10) + q.stalled(10, 10)).to_set.should eq(jids)
        
        client.config["heartbeat"] = 60
        jobs = q.pop(20)
        (q.running(0, 10) + q.running(10, 10)).to_set.should eq(jids)
        
        jids = 20.times.map { |i| q.put(Qless::Job, {"test" => "rssd"}, :delay => 60) }
        (q.scheduled(0, 10) + q.scheduled(10, 10)).to_set.should eq(jids.to_set)
        
        jids = 20.times.map { |i| q.put(Qless::Job, {"test" => "rssd"}, :depends => jids) }
        (q.depends(0, 10) + q.depends(10, 10)).to_set.should eq(jids.to_set)
      end
    end
    
    describe "#retry" do
      # It should decrement retries, and put it back in the queue. If retries
      # have been exhausted, then it should be marked as failed.
      # Prohibitions:
      #   1) We can't retry from another worker
      #   2) We can't retry if it's not running
      it "performs retry in the most basic way" do
        jid = q.put(Qless::Job, {'test' => 'retry'})
        job = q.pop
        job.retries.should eq(job.remaining)
        job.retry()
        # Pop is off again
        q.scheduled().should eq([])
        client.job(job.jid).state.should eq('waiting')
        job = q.pop
        job.should_not eq(nil)
        job.retries.should eq(job.remaining + 1)
        # Retry it again, with a backoff
        job.retry(60)
        q.pop.should eq(nil)
        q.scheduled.should eq([jid])
        job = client.job(jid)
        job.retries.should eq(job.remaining + 2)
        job.state.should eq('scheduled')
      end
      
      it "fails when we exhaust its retries through retry()" do
        jid = q.put(Qless::Job, {'test' => 'test_retry_fail'}, :retries => 2)
        client.failed.should eq({})
        q.pop.retry.should eq(1)
        q.pop.retry.should eq(0)
        q.pop.retry.should eq(-1)
        client.failed.should eq({'failed-retries-testing' => 1})
      end
      
      it "prevents us from retrying jobs not running" do
        job = client.job(q.put(Qless::Job, {'test' => 'test_retry_error'}))
        job.retry.should eq(false)
        q.pop.fail('foo', 'bar')
        client.job(job.jid).retry.should eq(false)
        client.job(job.jid).move('testing')
        job = q.pop;
        job.instance_variable_set(:@worker_name, 'foobar')
        job.retry.should eq(false)
        job.instance_variable_set(:@worker_name, Qless.worker_name)
        job.complete
        job.retry.should eq(false)
      end
      
      it "stops reporting a job as being associated with a worker when is retried" do
        jid = q.put(Qless::Job, {'test' => 'test_retry_workers'})
        job = q.pop
        client.workers(Qless.worker_name).should eq({'jobs' => [jid], 'stalled' => {}})
        job.retry.should eq(4)
        client.workers(Qless.worker_name).should eq({'jobs' => {}, 'stalled' => {}})
      end
    end
    
    describe "#priority" do
      # Basically all we need to test:
      # 1) If the job doesn't exist, then attempts to set the priority should
      #   return false. This doesn't really matter for us since we're using the
      #   __setattr__ magic method
      # 2) If the job's in a queue, but not yet popped, we should update its
      #   priority in that queue.
      # 3) If a job's in a queue, but already popped, then we just update the 
      #   job's priority.
      it "can manipulate priority midstream" do
        a = q.put(Qless::Job, {"test" => "priority"}, :priority => -10)
        b = q.put(Qless::Job, {"test" => "priority"})
        q.peek.jid.should eq(a)
        client.job(b).priority = -20
        q.length.should eq(2)
        q.peek.jid.should eq(b)
        job = q.pop
        q.length.should eq(2)
        job.jid.should eq(b)
        job = q.pop
        q.length.should eq(2)
        job.jid.should eq(a)
        job.priority = -30
        # Make sure it didn't get doubly-inserted into the queue
        q.length.should eq(2)
        q.peek.should eq(nil)
        q.pop.should eq(nil)
      end
    end
    
    describe "#tag" do
      # 1) Should make sure that when we double-tag an item, that we don't
      #   see it show up twice when we get it back with the job
      # 2) Should also preserve tags in the order in which they were inserted
      # 3) When a job expires or is canceled, it should be removed from the 
      #   set of jobs with that tag
      it "can tag in the most basic way" do
        job = client.job(q.put(Qless::Job, {"test" => "tag"}))
        client.tagged('foo').should eq({"total" => 0, "jobs" => {}})
        client.tagged('bar').should eq({"total" => 0, "jobs" => {}})
        job.tag('foo')
        client.tagged('foo').should eq({"total" => 1, "jobs" => [job.jid]})
        client.tagged('bar').should eq({"total" => 0, "jobs" => {}})
        job.tag('bar')
        client.tagged('foo').should eq({"total" => 1, "jobs" => [job.jid]})
        client.tagged('bar').should eq({"total" => 1, "jobs" => [job.jid]})
        job.untag('foo')
        client.tagged('foo').should eq({"total" => 0, "jobs" => {}})
        client.tagged('bar').should eq({"total" => 1, "jobs" => [job.jid]})
        job.untag('bar')
        client.tagged('foo').should eq({"total" => 0, "jobs" => {}})
        client.tagged('bar').should eq({"total" => 0, "jobs" => {}})
      end
      
      it "can preserve the order of tags" do
        job = client.job(q.put(Qless::Job, {"test" => "preserve_order"}))
        tags = %w{a b c d e f g h}
        tags.length.times do |i|
          job.tag(tags[i])
          client.job(job.jid).tags.should eq(tags[0..i])
        end
        
        # Now let's take a few out
        job.untag('a', 'c', 'e', 'g')
        client.job(job.jid).tags.should eq(%w{b d f h})
      end
      
      it "removes tags when canceling / expiring jobs" do
        job = client.job(q.put(Qless::Job, {"test" => "cancel_expire"}))
        job.tag("foo", "bar")
        client.tagged('foo').should eq({"total" => 1, "jobs" => [job.jid]})
        client.tagged('bar').should eq({"total" => 1, "jobs" => [job.jid]})
        job.cancel()
        client.tagged('foo').should eq({"total" => 0, "jobs" => {}})
        client.tagged('bar').should eq({"total" => 0, "jobs" => {}})
        
        # Now we have job expire from completion
        client.config['jobs-history-count'] = 0
        q.put(Qless::Job, {"test" => "cancel_expire"})
        job = q.pop
        job.should_not eq(nil)
        job.tag('foo', 'bar')
        client.tagged('foo').should eq({"total" => 1, "jobs" => [job.jid]})
        client.tagged('bar').should eq({"total" => 1, "jobs" => [job.jid]})
        job.complete
        client.tagged('foo').should eq({"total" => 0, "jobs" => {}})
        client.tagged('bar').should eq({"total" => 0, "jobs" => {}})
        
        # If the job no longer exists, attempts to tag it should not add to the set
        job.tag('foo', 'bar')
        client.tagged('foo').should eq({"total" => 0, "jobs" => {}})
        client.tagged('bar').should eq({"total" => 0, "jobs" => {}})
      end
      
      it "can tag a job when we initially put it on" do
        client.tagged('foo').should eq({"total" => 0, "jobs" => {}})
        client.tagged('bar').should eq({"total" => 0, "jobs" => {}})
        jid = q.put(Qless::Job, {'test' => 'tag_put'}, :tags => ['foo', 'bar'])
        client.tagged('foo').should eq({"total" => 1, "jobs" => [jid]})
        client.tagged('bar').should eq({"total" => 1, "jobs" => [jid]})
      end
      
      it "can return the top tags in use" do
        # 1) Make sure that it only includes tags with more than one job associated with it
        # 2) Make sure that when jobs are untagged, it decrements the count
        # 3) When we tag a job, it increments the count
        # 4) When jobs complete and expire, it decrements the count
        # 5) When jobs are put, make sure it shows up in the tags
        # 6) When canceled, decrements
        client.tags.should eq({})
        jids = 10.times.map { |x| q.put(Qless::Job, {}, :tags => ['foo']) }
        client.tags.should eq(['foo'])
        jids.each do |jid|
          client.job(jid).cancel
        end
        # Add only one back
        q.put(Qless::Job, {}, :tags => ['foo'])
        client.tags.should eq({})
        # Add a second, and tag it
        b = client.job(q.put(Qless::Job, {}))
        b.tag('foo')
        client.tags.should eq(['foo'])
        b.untag('foo')
        client.tags.should eq({})
        b.tag('foo')
        # Test job expiration
        client.config['jobs-history-count'] = 0
        q.length.should eq(2)
        q.pop.complete
        client.tags.should eq({})
      end
    end
    
    describe "#dependencies" do
      it "can recognize dependencies" do
        # In this test, we want to put a job, and put a second job
        # that depends on it. We'd then like to verify that it's 
        # only available for popping once its dependency has completed
        jid = q.put(Qless::Job, {"test" => "depends_put"})
        job = q.pop
        jid = q.put(Qless::Job, {"test" => "depends_put"}, :depends => [job.jid])
        q.pop.should eq(nil)
        client.job(jid).state.should eq('depends')
        job.complete
        client.job(jid).state.should eq('waiting')
        q.pop.jid.should eq(jid)
        
        # Let's try this dance again, but with more job dependencies
        jids = 10.times.map { |i| q.put(Qless::Job, { "test" => "depends_put" })}
        jid  = q.put(Qless::Job, { "test" => "depends_put" }, :depends => jids)
        # Pop more than we put on
        jobs = q.pop(20)
        jobs.length.should eq(10)
        # Complete them, and then make sure the last one's available
        jobs.each { |job| q.pop.should eq(nil); job.complete }
        # It's only when all the dependencies have been completed that
        # we should be able to pop this job off
        q.pop.jid.should eq(jid)
      end
      
      it "can add dependencies at completion, too" do
        # In this test, we want to put a job, put a second job, and
        # complete the first job, making it dependent on the second
        # job. This should test the ability to add dependency during
        # completion
        a = q.put(Qless::Job, { "test" => "depends_complete" })
        b = q.put(Qless::Job, { "test" => "depends_complete" })
        job = q.pop
        job.complete('testing', :depends => [b])
        client.job(a).state.should eq('depends')
        jobs = q.pop(20)
        jobs.length.should eq(1)
        jobs[0].complete
        client.job(a).state.should eq('waiting')
        job = q.pop
        job.jid.should eq(a)
        
        jids = 10.times.map { |i| q.put(Qless::Job, { "test" => "depends_complete" }) }
        jid  = job.jid
        job.complete('testing', :depends => jids)
        # Pop more than we put on
        jobs = q.pop(20)
        jobs.length.should eq(10)
        # Complete them, and then make sure the last one's available
        jobs.each { |job| q.pop.should eq(nil); job.complete }
        
        # It's only when all the dependencies have been completed that
        # we should be able to pop this job off
        q.pop.jid.should eq(jid)
      end
      
      it "can detect when dependencies are already satisfied" do
        # Put a job, and make it dependent on a canceled job, and a
        # non-existent job, and a complete job. It should be available
        # from the start.
        jids = ['foobar',
          q.put(Qless::Job, {"test" => "depends_state"}),
          q.put(Qless::Job, {"test" => "depends_state"})
        ]
        
        # Cancel one, complete one
        q.pop.cancel
        q.pop.complete
        q.length.should eq(0)
        jid = q.put(Qless::Job, {"test" => "depends_state"}, :depends => jids)
        q.pop.jid.should eq(jid)
      end
      
      it "deals with cancelation well with dependencies" do
        # B is dependent on A, but then we cancel B, then A is still
        # able to complete without any problems. If you try to cancel
        # a job that others depend on, you should have an exception thrown
        a = q.put(Qless::Job, {"test" => "depends_canceled"})
        b = q.put(Qless::Job, {"test" => "depends_canceled"}, :depends => [a])
        client.job(b).cancel
        job = q.pop
        job.jid.should eq(a)
        job.complete.should eq('complete')
        q.pop.should eq(nil)
        
        a = q.put(Qless::Job, {"test" => "depends_canceled"})
        b = q.put(Qless::Job, {"test" => "depends_canceled"}, :depends => [a])
        lambda { client.job(a).cancel }.should raise_error
      end
      
      it "unlocks a job only after its dependencies have completely finished" do
        # If we make B depend on A, and then move A through several
        # queues, then B should only be availble once A has finished
        # its whole run.
        a = q.put(Qless::Job, {"test" => "depends_advance"})
        b = q.put(Qless::Job, {"test" => "depends_advance"}, :depends => [a])
        10.times do |i|
          job = q.pop
          job.jid.should eq(a)
          job.complete("testing")
        end
        
        q.pop.complete
        q.pop.jid.should eq(b)
      end
      
      it "can support dependency chains" do
        # If we make a dependency chain, then we validate that we can
        # only access them one at a time, in the order of their dependency
        jids = [q.put(Qless::Job, {"test" => "cascading_dependency"})]
        10.times do |i|
          jids.push(q.put(Qless::Job, {"test" => "cascading_dependency"}, :depends => [jids[i]]))
        end
        
        11.times do |i|
          jobs = q.pop(10)
          jobs.length.should eq(1)
          jobs[0].jid.should eq(jids[i])
          jobs[0].complete
        end
      end
      
      it "carries dependencies when moved" do
        # If we put a job into a queue with dependencies, and then 
        # move it to another queue, then all the original dependencies
        # should be honored. The reason for this is that dependencies
        # can always be removed after the fact, but this prevents us
        # from the running the risk of moving a job, and it getting 
        # popped before we can describe its dependencies
        a = q.put(Qless::Job, {"test" => "move_dependency"})
        b = q.put(Qless::Job, {"test" => "move_dependency"}, :depends => [a])
        client.job(b).move("other")
        client.job(b).state.should eq("depends")
        other.pop.should eq(nil)
        q.pop.complete
        client.job(b).state.should eq("waiting")
        other.pop.jid.should eq(b)
      end
      
      it "supports adding dependencies" do
        # If we have a job that already depends on on other jobs, then
        # we should be able to add more dependencies. If it's not, then
        # we can't
        a = q.put(Qless::Job, {"test" => "add_dependency"})
        b = q.put(Qless::Job, {"test" => "add_dependency"}, :depends => [a])
        c = q.put(Qless::Job, {"test" => "add_dependency"})
        client.job(b).depend(c).should eq(true)
        
        jobs = q.pop(20)
        jobs.length.should eq(2)
        jobs[0].jid.should eq(a)
        jobs[1].jid.should eq(c)
        jobs[0].complete
        q.pop.should eq(nil)
        jobs[1].complete
        q.pop.jid.should eq(b)
        
        # If the job's put, but waiting, we can't add dependencies
        a = q.put(Qless::Job, {"test" => "add_dependency"})
        b = q.put(Qless::Job, {"test" => "add_depencency"})
        client.job(a).depend(b).should eq(false)
        job = q.pop
        job.depend(b).should eq(false)
        job.fail('what', 'something')
        client.job(job.jid).depend(b).should eq(false)
      end
      
      it "supports removing dependencies" do
        # If we have a job that already depends on others, then we should
        # we able to remove them. If it's not dependent on any, then we can't.        
        a = q.put(Qless::Job, {"test" => "remove_dependency"})
        b = q.put(Qless::Job, {"test" => "remove_dependency"}, :depends => [a])
        q.pop(20).length.should eq(1)
        client.job(b).undepend(a)
        q.pop.jid.should eq(b)
        
        # Let's try removing /all/ dependencies
        jids = 10.times.map { |i| q.put(Qless::Job, {"test" => "remove_dependency"}) }
        b = q.put(Qless::Job, {"test" => "remove_dependency"}, :depends => jids)
        q.pop(20).length.should eq(10)
        client.job(b).undepend(:all)
        client.job(b).state.should eq('waiting')
        q.pop.jid.should eq(b)
        jids.each do |jid|
          client.job(jid).dependents.should eq([])
        end
        
        a = q.put(Qless::Job, {"test" => "remove_dependency"})
        b = q.put(Qless::Job, {"test" => "remove_dependency"})
        client.job(a).undepend(b).should eq(false)
        job = q.pop
        job.undepend(b).should eq(false)
        job.fail('what', 'something')
        client.job(job.jid).undepend(b).should eq(false)
      end
      
      it "lets us see dependent jobs in a queue" do
        # When we have jobs that have dependencies, we should be able to
        # get access to them.
        a = q.put(Qless::Job, {"test" => "jobs_depends"})
        b = q.put(Qless::Job, {"test" => "jobs_depends"}, :depends => [a])
        client.queues[0]['depends'].should eq(1)
        client.queues('testing')['depends'].should eq(1)
        q.depends().should eq([b])
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
      
      it "checks priority's arguments" do
        priority = Qless::Lua.new("pop", @redis)
        [
          # Passing in keys
          [['foo'], ['12345', 1]],
          # Missing jid
          [[], []],
          # Missing priority
          [[], ['12345']],
          # Malformed priority
          [[], ['12345', 'howdy']]
        ].each { |x| lambda { priority(x[0], x[1]) }.should raise_error }
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
      
      it "checks retry's arguments" do
        rtry = Qless::Lua.new("queues", @redis)
        [
          # Passing in keys
          [['foo'], ['12345', 'testing', 'worker', 12345, 0]],
          # Missing jid
          [[], []],
          # Missing queue
          [[], ['12345']],
          # Missing worker
          [[], ['12345', 'testing']],
          # Missing now
          [[], ['12345', 'testing', 'worker']],
          # Malformed now
          [[], ['12345', 'testing', 'worker', 'howdy']],
          # Malformed delay
          [[], ['12345', 'testing', 'worker', 12345, 'howdy']]
        ].each { |x| lambda { rtry(x[0], x[1]) }.should raise_error }
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
      
      it "checks tags' arguments" do
        tag = Qless::Lua.new("tag", @redis)
        [
          # Passing in keys
          [['foo'], ['add', '12345', 12345, 'foo']],
          # First, test 'add' command
          # Missing command
          [[], []],
          # Missing jid
          [[], ['add']],
          # Missing now
          [[], ['add', '12345']],
          # Malformed now
          [[], ['add', '12345', 'howdy']],
          # Now, test 'remove' command
          # Missing jid
          [[], ['remove']],
          # Now, test 'get'
          # Missing tag
          [[], ['get']],
          # Malformed offset
          [[], ['get', 'foo', 'howdy']],
          # Malformed count
          [[], ['get', 'foo', 0, 'howdy']],
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
