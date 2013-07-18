require 'spec_helper'
require "qless"
require "redis"
require "json"
require 'yaml'
require 'qless/wait_until'

def Time.freeze()
  @_start = Time.now
  @_total = 0
  Time.stub!(:now).and_return(@_start)
end

def Time.advance(amount=0)
  @_total += amount
  Time.stub!(:now).and_return(Time.at(@_start.to_f + @_total))
end

module Qless
  class FooJob
    # An empty class
  end
  
  describe Qless::Client, :integration do
    # Our main test queue
    let(:q) { client.queues["testing"] }
    # Point to the main queue, but identify as different workers
    let(:a) { client.queues["testing"].tap { |o| o.worker_name = "worker-a" } }
    let(:b) { client.queues["testing"].tap { |o| o.worker_name = "worker-b" } }
    # And a second queue
    let(:other) { client.queues["other"]   }
    let(:lua_script) { Qless::LuaScript.new("qless", @redis) }
    # A helper for running the lua tests
    def lua_helper(command, args)
      args.each { |x| lambda { lua_script([], [command, 12345] + x) }.should raise_error }
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
    
    describe "#events", :uses_threads do
      let(:events    ) { Hash.new { |h, k| h[k] = ::Queue.new } }
      let(:pubsub    ) { new_client }
      let!(:thread   ) do
        Thread.new do
          pubsub.events.listen do |on|
            on.canceled  { |jid| events['canceled' ] << jid }
            on.completed { |jid| events['completed'] << jid }
            on.failed    { |jid| events['failed'   ] << jid }
            on.popped    { |jid| events['popped'   ] << jid }
            on.put       { |jid| events['put'      ] << jid }
            on.stalled   { |jid| events['stalled'  ] << jid }
            on.track     { |jid| events['track'    ] << jid }
            on.untrack   { |jid| events['untrack'  ] << jid }
          end
        end.tap { |t| t.join(0.01) }
      end

      let!(:tracked  ) { job = client.jobs[q.put(Qless::Job, {:foo => 'bar'})]; job.track; job }
      let!(:untracked) { job = client.jobs[q.put(Qless::Job, {:foo => 'bar'})]; job }

      def published_jids(type, expected_count)
        expected_count.times.each_with_object([]) do |time, list|
          list << events[type].pop
        end + try_pop_additional_events(type)
      end

      def try_pop_additional_events(type)
        # We don't expect additional events, but check to see if there
        # are some additional ones...
        3.times.map do
          sleep 0.005

          begin
            events[type].pop(:non_block)
          rescue ThreadError
            nil
          end
        end.compact
      end

      def should_only_have_tracked_jid_for(type)
        jids = published_jids(type, 1)
        jids.should include(tracked.jid)
        jids.should_not include(untracked.jid)
      end
      
      it "can pick up on canceled events" do
        # We should be able to see when tracked jobs are canceled
        tracked.cancel
        untracked.cancel
        should_only_have_tracked_jid_for 'canceled'
      end
      
      it "can pick up on completion events" do
        q.pop(10).each { |job| job.complete }
        should_only_have_tracked_jid_for 'completed'
      end
      
      it "can pick up on failed events" do
        q.pop(10).each { |job| job.fail('foo', 'bar') }

        should_only_have_tracked_jid_for 'failed'
      end
      
      it "can pick up on pop events" do
        q.pop(10)

        should_only_have_tracked_jid_for 'popped'
      end
      
      it "can pick up on put events" do
        tracked.move('other')
        untracked.move('other')

        should_only_have_tracked_jid_for 'put'
      end
      
      it "can pick up on stalled events" do
        client.config['grace-period'] = 0
        Time.freeze
        jobs = q.pop(2) # Pop them off
        jobs.length.should eq(2)
        Time.advance(600)
        jobs = q.pop(2) # And stall them
        jobs.length.should eq(2)
        (jobs[0].original_retries - jobs[0].retries_left).should eq(1)
        (jobs[1].original_retries - jobs[1].retries_left).should eq(1)

        should_only_have_tracked_jid_for 'stalled'
      end
      
      it "can pick up on track and untrack events" do
        tracked.untrack
        untracked.track

        track_jids = published_jids('track', 1)
        untrack_jids = published_jids('untrack', 1)

        untrack_jids.should include(tracked.jid)
        track_jids.should include(untracked.jid)
      end
    end

    specify "jobs keep track of their initial put time" do
      q.put(Qless::Job, {'foo' => 'bar'}, :priority => 10)
      job = q.pop

      expect(job.initially_put_at).to be_within(2).of(Time.now.getutc)
    end
    
    describe "#recur" do
      it "can use recur in the most basic way" do
        # In this test, we want to enqueue a job and make sure that
        # we can get some jobs from it in the most basic way. We should
        # get jobs out of the queue every _k_ seconds
        Time.freeze
        q.recur(Qless::Job, {'test' => 'test_recur_on'}, interval=1800)
        q.pop.complete.should eq('complete')
        q.pop.should eq(nil)
        Time.advance(1799)
        q.pop.should eq(nil)
        Time.advance(2)
        job = q.pop
        job.data.should eq({'test' => 'test_recur_on'})
        job.complete
        # We should not be able to pop a second job
        q.pop.should eq(nil)
        # Let's advance almost to the next one, and then check again
        Time.advance(1798)
        q.pop.should eq(nil)
        Time.advance(2)
        q.pop.should_not eq(nil)
      end

      it "can specify a jid in recur and klass as string" do
        client.queues['foo'].recur('Qless::Job',
          {'foo' => 'bar'}, 5, :jid => 'howdy').should eq('howdy')
        client.jobs['howdy'].should be
      end
      
      it "gives the jobs it spawns with the same attributes it has" do
        # Popped jobs should have the same priority, tags, etc. that the
        # recurring job has
        Time.freeze
        q.recur(Qless::Job, {'test' => 'test_recur_attributes'}, 100, :priority => -10, :tags => ['foo', 'bar'], :retries => 2)
        q.pop.complete.should eq('complete')
        10.times.each do |i|
          Time.advance(100)
          job = q.pop
          job.should_not      eq(nil)
          job.priority.should eq(-10)
          job.tags.should     eq(['foo', 'bar'])
          job.original_retries.should  eq(2)
          client.jobs.tagged('foo')['jobs'].should include(job.jid)
          client.jobs.tagged('bar')['jobs'].should include(job.jid)
          client.jobs.tagged('hey')['jobs'].should_not include(job.jid)
          job.complete
          q.pop.should eq(nil)
        end
      end
      
      it "should spawn a job only after offset and interval seconds" do
        # In this test, we should get a job after offset and interval
        # have passed
        Time.freeze
        q.recur(Qless::Job, {'test' => 'test_recur_offset'}, 100, :offset => 50)
        q.pop.should eq(nil)
        Time.advance(30)
        q.pop.should eq(nil)
        Time.advance(20)
        job = q.pop
        job.should be()
        job.complete
        # And henceforth we should get jobs periodically at 100 seconds
        Time.advance(99)
        q.pop.should eq(nil)
        Time.advance(2)
        q.pop.should be()
      end
      
      it "can cancel recurring jobs" do
        # In this test, we want to make sure that we can stop recurring
        # jobs
        # We should see these recurring jobs crop up under queues when 
        # we request them
        Time.freeze
        jid = q.recur(Qless::Job, {'test' => 'test_recur_off'}, 100)
        q.pop.complete.should eq('complete')
        client.queues.counts[0]['recurring'].should eq(1)
        client.queues['testing'].counts['recurring'].should eq(1)
        # Now, let's pop off a job, and then cancel the thing
        Time.advance(110)
        q.pop.complete.should eq('complete')
        job = client.jobs[jid]
        job.class.should eq(Qless::RecurringJob)
        job.cancel
        client.queues.counts[0]['recurring'].should eq(0)
        client.queues['testing'].counts['recurring'].should eq(0)
        Time.advance(1000)
        q.pop.should eq(nil)
      end
      
      it "can list all of the jids of recurring jobs" do
        # We should be able to list the jids of all the recurring jobs
        # in a queue
        jids = 10.times.map { |i| q.recur(Qless::Job, {'test' => 'test_jobs_recur'}, (i + 1) * 10) }
        q.jobs.recurring().should eq(jids)
        jids.each do |jid|
          client.jobs[jid].class.should eq(Qless::RecurringJob)
        end
      end

      it "includes recurring jobs with a future offset in the recurring jobs list" do
        jids = 3.times.map { |i| q.recur(Qless::Job, {'test' => 'test_jobs_recur'}, (i + 1) * 10, offset: (i + 1) * 10) }
        q.jobs.recurring().should eq(jids)
        jids.each do |jid|
          client.jobs[jid].class.should eq(Qless::RecurringJob)
        end
      end
      
      it "can get a recurring job" do
        # We should be able to get the data for a recurring job
        Time.freeze
        jid = q.recur(Qless::Job, {'test' => 'test_recur_get'}, 100, :priority => -10, :tags => ['foo', 'bar'], :retries => 2)
        job = client.jobs[jid]
        job.class.should      eq(Qless::RecurringJob)
        job.priority.should   eq(-10)
        job.queue_name.should eq('testing')
        job.data.should       eq({'test' => 'test_recur_get'})
        job.tags.should       eq(['foo', 'bar'])
        job.interval.should   eq(100)
        job.retries.should    eq(2)
        job.count.should      eq(0)
        job.klass_name.should eq('Qless::Job')
        # Now let's pop a job
        q.pop
        client.jobs[jid].count.should eq(1)
      end

      it "can preserve empty array job data" do
        job = client.jobs[q.recur(Qless::Job, {'test' => []}, 100)]
        job.data.should eq({'test' => []})

        # Make sure it works with updates, too
        job.data = {'testing' => []}
        job = client.jobs[job.jid]
        job.data.should eq({'testing' => []})
      end
      
      it "gives us multiple jobs" do
        # We should get multiple jobs if we've passed the interval time
        # several times.
        Time.freeze
        jid = q.recur(Qless::Job, {'test' => 'test_passed_interval'}, 100)
        q.pop.complete.should eq('complete')
        Time.advance(850)
        jobs = q.pop(100)
        jobs.length.should eq(8)
        jobs.each { |job| job.complete() }

        # If we are popping fewer jobs than the number of jobs that would have
        # been scheduled, it should only make that many available
        Time.advance(800)
        jobs = q.pop(5)
        jobs.length.should eq(5)
        q.length.should eq(5)
        jobs.each { |job| job.complete() }

        # Even if there are several recurring jobs, both of which need jobs
        # scheduled, it only pops off the needed number
        jid = q.recur(Qless::Job, {'test' => 'test_passed_interval_2'}, 10)
        Time.advance(500)
        jobs = q.pop(5)
        jobs.length.should eq(5)
        q.length.should eq(5)
        jobs.each { |job| job.complete() }

        # And if there are other jobs that are there, it should only move over
        # as many recurring jobs as needed
        jid = q.put(Qless::Job, {'foo' => 'bar'}, :priority => 10)
        jobs = q.pop(5)
        jobs.length.should eq(5)
        # Not sure why this is 6, but it's not a huge deal in my opinion
        q.length.should eq(6)
      end
      
      it "lists recurring job counts in the queues endpoint" do   
        # We should see these recurring jobs crop up under queues when 
        # we request them
        jid = q.recur(Qless::Job, {'test' => 'test_queues_endpoint'}, 100)
        client.queues.counts[0]['recurring'].should eq(1)
        client.queues['testing'].counts['recurring'].should eq(1)
      end
      
      it "can change attributes of a recurring crawl" do
        # We should be able to change the attributes of a recurring job,
        # and future spawned jobs should be affected appropriately. In
        # addition, when we change the interval, the effect should be 
        # immediate (evaluated from the last time it was run)
        Time.freeze
        jid = q.recur(Qless::Job, {'test' => 'test_change_attributes'}, 1)
        q.pop.complete.should eq('complete')
        job = client.jobs[jid]
        
        # First, priority
        Time.advance(1)
        q.pop.priority.should_not              eq(-10)
        client.jobs[jid].priority.should_not   eq(-10)
        job.priority = -10
        Time.advance(1)
        q.pop.priority.should                  eq(-10)
        client.jobs[jid].priority.should       eq(-10)
        
        # And data
        Time.advance(1)
        q.pop.data.should_not                  eq({'foo' => 'bar'})
        client.jobs[jid].data.should_not       eq({'foo' => 'bar'})
        job.data = {'foo' => 'bar'}
        Time.advance(1)
        q.pop.data.should                      eq({'foo' => 'bar'})
        client.jobs[jid].data.should           eq({'foo' => 'bar'})
        
        # And retries
        Time.advance(1)
        q.pop.original_retries.should_not      eq(10)
        client.jobs[jid].retries.should_not    eq(10)
        job.retries = 10
        Time.advance(1)
        q.pop.original_retries.should          eq(10)
        client.jobs[jid].retries.should        eq(10)
        
        # And klass
        Time.advance(1)
        q.pop.klass.should_not                 eq(Qless::RecurringJob)
        client.jobs[jid].klass_name.should_not eq('Qless::RecurringJob')
        job.klass = Qless::RecurringJob
        Time.advance(1)
        q.pop.klass.should                     eq(Qless::RecurringJob)
        client.jobs[jid].klass_name.should     eq('Qless::RecurringJob')
      end
      
      it "can let us change the interval" do
        # If we update a recurring job's interval, then we should get
        # jobs from it as if it had been scheduled this way from the
        # last time it had a job popped
        Time.freeze
        jid = q.recur(Qless::Job, {'test' => 'test_change_interval'}, 100)
        q.pop.complete.should eq('complete')
        Time.advance(100)
        q.pop.complete.should eq('complete')
        Time.advance(50)
        # Now let's update to make it more frequent
        client.jobs[jid].interval = 10
        jobs = q.pop(100)
        jobs.length.should eq(5)
        jobs.each { |job| job.complete }
        # Now let's make the interval much longer
        Time.advance(49) ; client.jobs[jid].interval = 1000; q.pop.should eq(nil)
        Time.advance(100); client.jobs[jid].interval = 1000; q.pop.should eq(nil)
        Time.advance(849); client.jobs[jid].interval = 1000; q.pop.should eq(nil)
        Time.advance(1)  ; client.jobs[jid].interval = 1000; q.pop.should eq(nil)
        Time.advance(2)  ; q.pop.should be
      end
      
      it "can let us move a recurring job from one queue to another" do
        # If we move a recurring job from one queue to another, then
        # all future spawned jobs should be popped from that queue
        Time.freeze
        jid = q.recur(Qless::Job, {'test' => 'test_move'}, 100)
        q.pop.complete.should eq('complete')
        Time.advance(110)
        q.pop.complete.should eq('complete')
        other.pop.should eq(nil)
        # Now let's move it to another queue
        client.jobs[jid].move('other')
        q.pop.should     eq(nil)
        other.pop.should eq(nil)
        Time.advance(100)
        q.pop.should     eq(nil)
        other.pop.complete.should eq('complete')
        client.jobs[jid].queue_name.should eq('other')
      end
      
      it "can update tags for the recurring job appropriately" do
        # We should be able to add and remove tags from a recurring job,
        # and see the impact in all the jobs it subsequently spawns
        Time.freeze
        jid = q.recur(Qless::Job, {'test' => 'test_change_tags'}, 1, :tags => ['foo', 'bar'])
        q.pop.complete.should eq('complete')
        Time.advance(1)
        q.pop.tags.should eq(['foo', 'bar'])
        # Now let's untag the job
        client.jobs[jid].untag('foo')
        client.jobs[jid].tags.should eq(['bar'])
        Time.advance(1)
        q.pop.tags.should eq(['bar'])
        
        # Now let's add 'foo' and 'hey' in
        client.jobs[jid].tag('foo', 'hey')
        client.jobs[jid].tags.should eq(['bar', 'foo', 'hey'])
        Time.advance(1)
        q.pop.tags.should eq(['bar', 'foo', 'hey'])
      end
      
      it "can peek at recurring jobs" do
        # When we peek at jobs in a queue, it should take recurring jobs
        # into account
        Time.freeze
        jid = q.recur(Qless::Job, {'test' => 'test_peek'}, 100)
        q.pop.complete.should eq('complete')
        q.peek.should eq(nil)
        Time.advance(110)
        q.peek.should_not eq(nil)
        q.pop.complete.should eq('complete')

        # If we are popping fewer jobs than the number of jobs that would have
        # been scheduled, it should only make that many available
        Time.advance(800)
        jobs = q.peek(5)
        jobs.length.should eq(5)
        q.length.should eq(5)
        q.pop(100).each { |job| job.complete() }
        q.length.should eq(0)

        # Even if there are several recurring jobs, both of which need jobs
        # scheduled, it only pops off the needed number
        jid = q.recur(Qless::Job, {'test' => 'test_passed_interval_2'}, 10)
        Time.advance(800)
        jobs = q.peek(5)
        jobs.length.should eq(5)
        q.length.should eq(5)
        q.pop(100).each { |job| job.complete() }
        q.length.should eq(0)

        # And if there are other jobs that are there, it should only move over
        # as many recurring jobs as needed
        Time.advance(800)
        jid = q.put(Qless::Job, {'foo' => 'bar'}, :priority => 10)
        jobs = q.peek(5)
        jobs.length.should eq(5)
        # Not sure why this is 6, but it's not a huge deal in my opinion
        q.length.should eq(6)
      end

      it "uses the time when it would have been scheduled in the history" do
        # If we pop or peek after waiting several intervals, then we should 
        # see the time it would have been put in the queue in the history
        Time.freeze
        start = Time.now.to_i
        jid = q.recur(Qless::Job, {'test' => 'test_passed_interval'}, 10)
        Time.advance(55)
        jobs = q.pop(100)
        jobs.length.should eq(6)
        6.times do |i|
          jobs[i].raw_queue_history[0]['what'].should eq('put')
          jobs[i].raw_queue_history[0]['when'].should eq(start + i * 10)
        end
        # Cancel the original rcurring job, complete these jobs, start for peek
        client.jobs[jid].cancel
        jobs.each { |job| job.complete.should eq('complete') }

        # Testing peek
        start = Time.now.to_i
        jid = q.recur(Qless::Job, {'test' => 'test_passed_interval'}, 10)
        Time.advance(55)
        jobs = q.peek(100)
        jobs.length.should eq(6)
        6.times do |i|
          jobs[i].raw_queue_history[0]['what'].should eq('put')
          jobs[i].raw_queue_history[0]['when'].should eq(start + i * 10)
        end
      end

      it "does not re-set the jid counter when re-recurring a job" do
        q.recur(Qless::Job, {'test' => 'test_passed_interval'}, 10,
            :jid => 'my_recurring_job')
        job1 = q.pop
        Time.freeze
        q.recur(Qless::Job, {'test' => 'test_passed_interval'}, 10,
            :jid => 'my_recurring_job')
        Time.advance(15)
        job2 = q.pop

        job1.jid.should_not eq(job2.jid)
      end

      it "updates the attributes of the job in re-recurring a job" do
        jid = q.recur(Qless::Job, {'test' => 'test_recur_update'}, 10,
            :jid => 'my_recurring job', :priority => 10, :tags => ['foo'],
            :retries => 5)
        # Now, let's /re-recur/ the thing, and make sure that its properties
        # have indeed updated as expected
        jid = q.recur(Qless::Job, {'test' => 'test_recur_update_2'}, 20,
            :jid => 'my_recurring job', :priority => 20, :tags => ['bar'],
            :retries => 10)
        job = client.jobs[jid]
        job.data.should eq({'test' => 'test_recur_update_2'})
        job.interval.should eq(20)
        job.priority.should eq(20)
        job.retries.should eq(10)
        job.tags.should eq(['bar'])
      end

      it "is a 'move' if you reput it into a different queue" do
        jid = q.recur(Qless::Job, {'test' => 'test_recur_update'}, 10,
            :jid => 'my_recurring_job')
        # Make sure it's in the queue
        stats = client.queues.counts.select { |s| s['name'] == q.name }
        stats.length.should eq(1)
        stats[0]['recurring'].should eq(1)

        # And we'll reput it into another queue
        jid = other.recur(Qless::Job, {'test' => 'test_recur_update'}, 10,
            :jid => 'my_recurring_job')
        # Make sure it's in the queue
        stats = client.queues.counts.select { |s| s['name'] == q.name }
        stats.length.should eq(1)
        stats[0]['recurring'].should eq(0)
        # Make sure it's in the queue
        stats = client.queues.counts.select { |s| s['name'] == other.name }
        stats.length.should eq(1)
        stats[0]['recurring'].should eq(1)
      end

      it "can limit the number of spawned jobs" do
        Time.freeze
        jid = q.recur(Qless::Job, {'test' => 'test_recur_backlog'}, 10,
          :jid => 'my_recurring', :backlog => 1)
        # Advance far enough that we'd normally spawn a lot of jobs
        5.times do
          Time.advance(105)
          jobs = q.pop(100)
          jobs.length.should eq(1)
          jobs.each { |job| job.complete }
        end

        client.jobs[jid].backlog = 5
        5.times do
          Time.advance(105)
          jobs = q.pop(100)
          jobs.length.should eq(5)
          jobs.each { |job| job.complete }
        end

        # After we update the backlog, it should go back to normal behavior,
        # and not just have a really long backlog
        client.jobs[jid].backlog = 0
        Time.advance(100)
        q.pop(100).length.should eq(10)
      end

      it "can update the backlog property" do
        jid = q.recur(Qless::Job, {}, 10, :jid => 'foo')
        client.jobs[jid].backlog.should eq(0)
        client.jobs[jid].backlog = 5
        client.jobs[jid].backlog.should eq(5)
      end
    end

    context "when there is a max concurrency set on the queue" do
      before do
        q.max_concurrency = 2
        q.heartbeat = 60
      end

      it 'exposes a reader method for the config value' do
        expect(q.max_concurrency).to eq(2)
      end

      it 'limits the number of jobs that can be worked on concurrently from that queue' do
        3.times { q.put(Qless::Job, {"test" => "put_get"}) }

        j1, j2 = 2.times.map { q.pop }
        expect(j1).to be_a(Qless::Job)
        expect(j2).to be_a(Qless::Job)

        2.times { expect(q.pop).to be_nil }

        j1.complete

        expect(q.pop).to be_a(Qless::Job)
      end

      it 'can still timeout the jobs' do
        client.config['grace-period'] = 0
        Time.freeze

        4.times { q.put(Qless::Job, {"test" => "put_get"}) }

        # Reach the max concurrency
        j1, j2 = 2.times.map do
          Time.advance(30)
          q.pop
        end

        expect(j1).to be_a(Qless::Job)
        expect(j2).to be_a(Qless::Job)
        q.peek
        expect(j1.retries_left).to eq(5)

        # Simulate a heartbeat timeout;
        # it should be able to pop a job now
        Time.advance(35)
        q.peek
        job = q.pop
        q.peek
        job.jid.should eq(j1.jid)
        expect(job).to be_a(Qless::Job)
        expect(job.retries_left).to eq(4)

        # But now it can't pop another one; it's at the max again.
        # ...but it should still be able to peek
        expect(q.pop).to be_nil
        # Just make sure that max_concurrency doesn't affect peek.
        expect(q.peek).to be_a(Qless::Job)

        # Once again, the job times out.
        Time.advance(60)
        job = q.pop
        expect(job).to be_a(Qless::Job)
        expect(job.retries_left).to eq(4)
      end

      it 'works even when reducing the max concurrency' do
        # If we pop off a bunch of jobs and then constrict the max concurrency,
        # then we can still complete the jobs, and can't pop off new jobs until
        # we've completed all of them
        q.max_concurrency = 100
        10.times { q.put(Qless::Job, {}) }
        jobs = q.pop(5)

        jobs.length.should eq(5)
        q.max_concurrency = 1
        jobs.each do |job|
          # Can't pop -- we still have them running
          q.pop.should_not be
          job.complete.should eq('complete')
        end

        # Now we should have some room
        q.pop.should be
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
        job = client.jobs[jid]
        job.priority.should        eq(0)
        job.data.should            eq({"test" => "put_get"})
        job.tags.should            eq([])
        job.worker_name.should          eq("")
        job.state.should           eq("waiting")
        job.raw_queue_history.length.should  eq(1)
        job.raw_queue_history[0]['q'].should eq("testing")
      end

      it "can specify a jid in put and klass as string" do
        client.queues['foo'].put('Qless::Job',
          {'foo' => 'bar'}, :jid => 'howdy').should eq('howdy')
        client.jobs['howdy'].should be
      end

      it "supports empty arrays in job data" do
        jid = q.put(Qless::Job, {"test"=>[]})
        job = client.jobs[jid]
        job.data.should eq({"test"=>[]})
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
        job.data.should         eq({'test' => 'test_put_pop_attributes'})
        job.worker_name.should  eq(Qless.worker_name)
        job.expires_at.should   > (Time.new.to_i - 20)
        job.state.should        eq('running')
        job.queue_name.should   eq('testing')
        job.retries_left.should eq(5)
        job.original_retries.should   eq(5)
        job.jid.should          eq(jid)
        job.klass.should        eq(Qless::Job)
        job.klass_name.should   eq('Qless::Job')
        job.tags.should         eq([])
        jid = q.put(Qless::FooJob, 'test' => 'test_put_pop_attributes')
        job = q.pop
        job.klass.should        eq(Qless::FooJob)
        job.klass_name.should   eq('Qless::FooJob')
      end

      it "can get all the attributes it expects after peeking" do
        # In this test, we want to put a job, peek a job, and make
        # sure that when peeks, we get all the attributes back 
        # that we expect
        #   1) put a job
        #   2) peek said job, check existence of attributes
        jid = q.put(Qless::Job, {'test' => 'test_put_pop_attributes'})
        job = q.peek
        job.data.should         eq({'test' => 'test_put_pop_attributes'})
        job.worker_name.should  eq('')
        job.state.should        eq('waiting')
        job.queue_name.should   eq('testing')
        job.retries_left.should eq(5)
        job.jid.should          eq(jid)
        job.klass.should        eq(Qless::Job)
        job.klass_name.should   eq('Qless::Job')
        job.tags.should         eq([])
        job.original_retries.should eq(5)
        jid = q.put(Qless::FooJob, 'test' => 'test_put_pop_attributes')
        q.pop; job = q.peek
        
        job.klass.should        eq(Qless::FooJob)
        job.klass_name.should   eq('Qless::FooJob')
      end
      
      it "can do data access as we expect" do
        # In this test, we'd like to make sure that all the data attributes
        # of the job can be accessed through __getitem__
        #   1) Insert a job
        #   2) Get a job,  check job['test']
        #   3) Peek a job, check job['test']
        #   4) Pop a job,  check job['test']
        job = client.jobs[q.put(Qless::Job, {"test" => "data_access"})]
        job["test"].should eq("data_access")
        q.peek["test"].should eq("data_access")
        q.pop[ "test"].should eq("data_access")
      end
      
      it "can handle priority correctly" do
        # In this test, we're going to add several jobs and make
        # sure that we get them in an order based on priority
        #   1) Insert 10 jobs into the queue with successively more priority
        #   2) Pop all the jobs, and ensure that with each pop we get the right one
        jids = 10.times.collect { |x| q.put(Qless::Job, {"test" => "put_pop_priority", "count" => x}, :priority => x)}
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
        job = client.jobs[jid]
        job.raw_queue_history[0]['what'].should eql('put')
        job.raw_queue_history[0]['when'].should be_within(2).of(Time.now.to_i)
        job = q.pop
        job = client.jobs[jid]
        job.raw_queue_history[1]['what'].should eql('popped')
        job.raw_queue_history[1]['when'].should be_within(2).of(Time.now.to_i)
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
        job = client.jobs[q.put(Qless::Job, {"test" => "move_queues"})]
        q.length.should     eq(1)
        other.length.should eq(0)
        job.move("other")
        q.length.should     eq(0)
        other.length.should eq(1)
      end

      it "preserves data unless overridden" do
        q.put(Qless::Job, {}, jid: "abc")
        q.put(Qless::Job, {}, jid: "123")
        q.put(Qless::Job, {}, jid: "456")

        jid = q.put(Qless::Job, {},
          :priority => 5, :tags => ['foo'], :retries => 10,
          :depends => %w[ abc 123 ])

        client.jobs[jid].move('bar')
        job = client.jobs[jid]
        # Make sure all the properties have been left unaltered
        job.retries_left.should eq(10)
        job.tags.should         eq(['foo'])
        job.priority.should     eq(5)
        job.data.should         eq({})
        job.dependencies.should match_array(%w[ abc 123 ])
        
        # Now we'll move it again, but override attributes
        job.move('foo', :data => {'foo' => 'bar'},
          :priority => 10, :tags => ['bar'], :retries => 20,
          :depends => %w[ 456 ])

        job = client.jobs[jid]
        job.retries_left.should eq(20)
        job.tags.should         eq(['bar'])
        job.priority.should     eq(10)
        job.data.should         eq({'foo' => 'bar'})
        job.dependencies.should match_array(%w[ abc 123 456 ])

        # We should also make sure that tags are updated so that the job is no
        # longer tagged 'foo'
        client.jobs.tagged('foo').should eq({"total" => 0, "jobs" => {}})
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
        expect {
          job.heartbeat
        }.to raise_error(Qless::LuaScriptError, /not currently running/)
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
        before = client.jobs[jid]
        before.move("other")
        after  = client.jobs[jid]
        before.tags.should     eq(["foo", "bar"])
        before.priority.should eq(5)
        before.tags.should     eq(after.tags)
        before.data.should     eq(after.data)
        before.priority.should eq(after.priority)
        after.raw_queue_history.length.should eq(2)
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
        ajob.heartbeat.should be_a(Integer)
        ajob.heartbeat.should >= Time.now.to_i
        # Try setting a queue-specific heartbeat
        q.heartbeat = -60
        ajob.heartbeat.should be_a(Integer)
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
        expect {
          client.jobs[jid].heartbeat
        }.to raise_error(Qless::LuaScriptError, /not currently running/)
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
        job = client.jobs[q.put(Qless::Job, {"test" => "put_failed"})]
        job = q.pop
        job.fail("foo", "some message")
        client.jobs.failed.should eq({"foo" => 1})
        job.move("testing")
        q.length.should eq(1)
        client.jobs.failed.should eq({})
      end

      it "can't fail a canceled/expired job" do
        ajob = client.jobs[q.put(Qless::Job, {})]
        bjob = client.jobs[ajob.jid]
        ajob.cancel()
        expect {
          bjob.fail('foo', 'bar')
        }.to raise_error(Qless::LuaScriptError, /does not/)
      end
      
      it "fails jobs correctly" do
        # In this test, we want to make sure that we can correctly 
        # fail a job
        #   1) Put a job
        #   2) Fail a job
        #   3) Ensure the queue is empty, and that there's something
        #       in the failed endpoint
        #   4) Ensure that the job still has its original queue
        client.jobs.failed.length.should eq(0)
        jid = q.put(Qless::Job, {"test" => "fail_failed"})
        job = q.pop
        job.fail("foo", "some message")
        q.pop.should       eq(nil)
        client.jobs.failed.should eq({"foo" => 1})
        results = client.jobs.failed("foo")
        results["total"].should         eq(1)
        job = results["jobs"][0]
        job.jid.should          eq(jid)
        job.queue_name.should   eq("testing")
        job.data.should         eq({"test" => "fail_failed"})
        job.worker_name.should  eq("")
        job.state.should        eq("failed")
        job.retries_left.should eq(5)
        job.klass.should        eq(Qless::Job)
        job.klass_name.should   eq('Qless::Job')
        job.tags.should         eq([])
        job.original_retries.should eq(5)
      end
      
      it "keeps us from completing jobs that we've failed" do
        # In this test, we want to make sure that we can pop a job,
        # fail it, and then we shouldn't be able to complete /or/ 
        # heartbeat the job
        #   1) Put a job
        #   2) Fail a job
        #   3) Heartbeat to job fails
        #   4) Complete job fails
        client.jobs.failed.length.should eq(0)
        jid = q.put(Qless::Job, {"test" => "pop_fail"})
        job = q.pop
        job.fail("foo", "some message")
        q.length.should eq(0)
        expect {
          job.heartbeat
        }.to raise_error(Qless::LuaScriptError, /not currently running/)

        expect {
          job.complete
        }.to raise_error(Qless::Job::CantCompleteError, /failed/)

        client.jobs.failed.should eq({"foo" => 1})
        results = client.jobs.failed("foo")
        results["total"].should      eq(1)
        results["jobs"][0].jid.should eq(jid)
      end

      it 'invokes before_complete but not after_complete on a job that has already been completed' do
        q.put(Qless::Job, {"test" => "pop_fail"})
        job = q.pop
        job.fail("foo", "some message")

        events = []
        job.before_complete { events << :before }
        job.after_complete  { events << :after  }

        expect {
          job.complete
        }.to raise_error(Qless::Job::CantCompleteError, /failed/)

        expect(events).to eq([:before])
      end
      
      it "keeps us from failing a job that's already completed" do
        # Make sure that if we complete a job, we cannot fail it.
        #   1) Put a job
        #   2) Pop a job
        #   3) Complete said job
        #   4) Attempt to fail job fails
        client.jobs.failed.length.should eq(0)
        jid = q.put(Qless::Job, {"test" => "fail_complete"})
        job = q.pop
        job.complete.should eq('complete')
        client.jobs[jid].state.should eq('complete')
        expect {
          job.fail("foo", "some message")
        }.to raise_error(Qless::LuaScriptError, /not currently running/)
        client.jobs.failed.length.should eq(0)
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
        client.jobs[jid].failure.should eq({})
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
        Time.freeze
        jid = q.put(Qless::Job, {"test" => "locks"})
        # Reset our heartbeat for both A and B
        client.config["heartbeat"] = -10
        # Make sure a gets a job
        ajob = a.pop
        bjob = b.pop
        # We've just sent the warning to the a worker
        bjob.should_not be
        Time.advance(30)
        bjob = b.pop
        ajob.jid.should eq(bjob.jid)
        bjob.heartbeat.should be_a(Integer)
        (bjob.heartbeat + 11).should > Time.now.to_i
        expect {
          ajob.heartbeat
        }.to raise_error(Qless::LuaScriptError, /handed out to another/)
      end

      it "removes jobs from original worker's list of jobs" do
        # When a worker loses a lock on a job, that job should be removed
        # from the list of jobs owned by that worker
        Time.freeze
        jid = q.put(Qless::Job, {"test" => "locks"}, :retries => 1)
        client.config["heartbeat"]    = -10
        client.config["grace-period"] = 20

        ajob = a.pop
        # Get the workers
        workers = Hash[client.workers.counts.map { |w| [w['name'], w] } ]
        workers[a.worker_name]["stalled"].should eq(1)

        # Should have one more retry, so we should be good
        bjob = b.pop
        Time.advance(30) # We have to wait for the grace period to expire
        bjob = b.pop
        workers = Hash[client.workers.counts.map { |w| [w['name'], w] } ]
        workers[a.worker_name]["stalled"].should eq(0)
        workers[b.worker_name]["stalled"].should eq(1)

        # Now it's automatically failed. Shouldn't appear in either worker
        bjob = b.pop
        workers = Hash[client.workers.counts.map { |w| [w['name'], w] } ]
        workers[a.worker_name]["stalled"].should eq(0)
        workers[b.worker_name]["stalled"].should eq(0)
      end

      it "can repeatedly give a grace period" do
        Time.freeze
        client.config["grace-period"] = 10
        client.config["heartbeat"]    = 10
        jid = q.put(Qless::Job, {})
        
        # At this point, we should be able to pop it, wait 10 seconds and see
        # no more jobs available (meaning the grace period has begun)
        4.times do |i|
          job = q.pop
          job.should be
          Time.advance(10)       # Time out the job
          q.pop.should_not be
          Time.advance(10)       # Wait for the grace period
        end
      end

      it "can be failed during the grace period" do
        Time.freeze
        client.config["grace-period"] = 10
        client.config["heartbeat"]    = 10
        jid = q.put(Qless::Job, {})

        # Now, when in the midst of the grace period, we should be able to fail
        # the job
        job = q.pop
        job.should be
        Time.advance(10)         # Time out the job
        job.fail('foo', 'bar')
        client.jobs[jid].state.should eq('failed')
      end

      it "can invalidate locks with timeout" do
        q.put(Qless::Job, {"test" => "locks"}, :retries => 5)
        ajob = a.pop
        a.pop.should eq(nil)
        ajob.retries_left.should eq(5)
        ajob.timeout
        # After the timeout, we should see ajob put, pop, timeout
        client.jobs[ajob.jid].raw_queue_history.length.should eql(3)
        job = a.pop
        job.should be
        # Now, we should only see the new pop event
        job.raw_queue_history.length.should eql(4)
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
        job = client.jobs[jid]
        q.length.should eq(1)
        job.cancel
        q.length.should eq(0)
        client.jobs[jid].should eq(nil)
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
        expect {
          job.heartbeat
        }.to raise_error(Qless::LuaScriptError, /does not exist/)
        expect {
          job.complete
        }.to raise_error(Qless::Job::CantCompleteError, /does not exist/)
        client.jobs[ jid].should eq(nil)
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
        client.jobs.failed.should eq({"foo" => 1})
        job.cancel
        client.jobs.failed.should eq({})
      end

      it "doesn't error when canceling an failed-retries job" do
        jid = q.put(Qless::Job, {"test" => "foo"}, :retries => 0)
        job = q.pop
        job.state.should eq('running')
        job.retry()
        client.jobs[jid].state.should eq('failed')
        expect { job.cancel }.to_not raise_error
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
        job = client.jobs[jid]
        # Should habe history for put, pop, complete
        job.raw_queue_history.length.should eq(3)
        job.state.should  eq("complete")
        job.worker_name.should eq("")
        job.queue_name.should  eq("")
        q.length.should  eq(0)
        
        # If we put it into another queue, it shouldn't appear in the complete
        # endpoint anymore
        job.move("testing")
        client.jobs.complete.should eq([])
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
        job = client.jobs[jid]
        # Should have history for put, pop, completion, and moving
        job.raw_queue_history.length.should eq(4)
        job.state.should  eq("waiting")
        job.worker_name.should eq("")
        job.queue_name.should  eq("testing")
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
        client.config['grace-period'] = 0
        ajob = a.pop
        ajob.jid.should eq(jid)
        bjob = b.pop
        bjob.jid.should eq(jid)

        expect {
          ajob.complete
        }.to raise_error(Qless::Job::CantCompleteError)
        expect { bjob.complete }.not_to raise_error

        job = client.jobs[jid]
        # Should include history for put, pop, timed-out, pop, and a complete
        job.raw_queue_history.length.should eq(5)
        job.raw_queue_history[2]['what'].should eq('timed-out')
        job.state.should  eq("complete")
        job.worker_name.should eq("")
        job.queue_name.should  eq("")
        q.length.should  eq(0)
      end
      
      it "can allow only popped jobs to be completed" do
        # In this test, we want to make sure that if we try to complete
        # a job that's in anything but the 'running' state.
        #   1) Put an item in a queue
        #   2) DO NOT pop that item from the queue
        #   3) Attempt to complete the job, ensure it fails
        jid = q.put(Qless::Job, "test" => "complete_fail")
        job = client.jobs[jid]

        expect {
          job.complete("testing")
        }.to raise_error(Qless::Job::CantCompleteError)

        expect {
          job.complete
        }.to raise_error(Qless::Job::CantCompleteError)
      end
      
      it "can ensure that the next queue appears in the queues endpoint" do
        # In this test, we want to make sure that if we complete a job and
        # advance it, that the new queue always shows up in the 'queues'
        # endpoint.
        #   1) Put an item in a queue
        #   2) Complete it, advancing it to a different queue
        #   3) Ensure it appears in 'queues'
        jid = q.put(Qless::Job, {"test" => "complete_queues"})
        client.queues.counts.select { |q| q["name"] == "other" }.length.should eq(0)
        q.pop.complete("other").should eq("waiting")
        client.queues.counts.select { |q| q["name"] == "other" }.length.should eq(1)
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
        client.jobs[jid].state.should eq("scheduled")
        Time.stub!(:now).and_return(start + 11)
        q.peek.state.should eq("waiting")
        client.jobs[jid].state.should eq("waiting")
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
        @redis.keys("ql:j:*").length.should eq(20)        
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
        stats["wait"]["mean" ].should be_within(0.0001).of(9.5)
        stats["wait"]["std"  ].should be_within(1e-8).of(5.916079783099)
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
        stats["run"]["mean" ].should be_within(0.0001).of(9.5)
        stats["run"]["std"  ].should be_within(1e-8).of(5.916079783099)
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
        client.jobs[jid].move("testing")
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
        job = client.jobs[q.put(Qless::Job, {"test" => "stats_failed_original_day"})]
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
        client.queues.counts.should eq({})
        q.put(Qless::Job, {"test" => "queues"}, :delay => 10)
        expected = {
          "name"      => "testing",
          "stalled"   => 0,
          "waiting"   => 0,
          "running"   => 0,
          "scheduled" => 1,
          "depends"   => 0,
          "recurring" => 0,
          "paused"    => false
        }
        client.queues.counts.should eq([expected])
        client.queues["testing"].counts.should eq(expected)
        
        q.put(Qless::Job, {"test" => "queues"})
        expected["waiting"] += 1
        client.queues.counts.should eq([expected])
        client.queues["testing"].counts.should eq(expected)
        
        job = q.pop
        expected["waiting"] -= 1
        expected["running"] += 1
        client.queues.counts.should eq([expected])
        client.queues["testing"].counts.should eq(expected)
        
        q.put(Qless::Job, {"test" => "queues"})
        client.config["heartbeat"] = -10
        job = q.pop
        expected["stalled"] += 1
        client.queues.counts.should eq([expected])
        client.queues["testing"].counts.should eq(expected)

        q.pause()
        expected['paused'] = true
        client.queues.counts.should eq([expected])
        client.queues["testing"].counts.should eq(expected)
      end
    end
    
    describe "#track" do
      it "can start tracking a job" do
        # In this test, we want to make sure that tracking works as expected.
        #   1) Check tracked jobs, expect none
        #   2) Put, Track a job, check
        #   3) Untrack job, check
        #   4) Track job, cancel, check
        client.jobs.tracked.should eq({"expired" => {}, "jobs" => []})
        job = client.jobs[q.put(Qless::Job, {"test" => "track"})]
        job.track
        client.jobs.tracked["jobs"].length.should eq(1)
        job.untrack
        client.jobs.tracked["jobs"].length.should eq(0)
        job.track
        job.cancel
        client.jobs.tracked["expired"].should eq([job.jid])
      end
      
      it "knows when a job is tracked" do
        # When peeked, popped, failed, etc., qless should know when a 
        # job is tracked or not
        # => 1) Put a job, track it
        # => 2) Peek, ensure tracked
        # => 3) Pop, ensure tracked
        # => 4) Fail, check failed, ensure tracked
        job = client.jobs[q.put(Qless::Job, {"test" => "track_tracked"})]
        job.track
        q.peek.tracked.should eq(true)
        job = q.pop
        job.tracked.should eq(true)
        job.fail("foo", "bar")
        client.jobs.failed("foo")["jobs"][0].tracked.should eq(true)
      end
      
      it "knows when a job is not tracked" do
        # When peeked, popped, failed, etc., qless should know when a 
        # job is not tracked
        # => 1) Put a job
        # => 2) Peek, ensure tracked
        # => 3) Pop, ensure tracked
        # => 4) Fail, check failed, ensure tracked
        job = client.jobs[q.put(Qless::Job, {"test" => "track_tracked"})]
        q.peek.tracked.should eq(false)
        job = q.pop
        job.tracked.should eq(false)
        job.fail("foo", "bar")
        client.jobs.failed("foo")["jobs"][0].tracked.should eq(false)
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
        client.jobs.failed.should eq({})
        q.put(Qless::Job, {"test" => "retries"}, :retries => 2)
        client.config["heartbeat"] = -10
        client.config['grace-period'] = 0
        q.pop; client.jobs.failed.should eq({})
        q.pop; client.jobs.failed.should eq({})
        q.pop; client.jobs.failed.should eq({})
        q.pop; client.jobs.failed.should eq({
          "failed-retries-testing" => 1
        })
      end

      it "can be moved if failed with retries exhausted" do
        client.config["heartbeat"] = -10
        client.config["grace-period"] = 0
        Time.freeze
        q.put(Qless::Job, {}, :retries => 5)
        job = q.pop
        5.times do |i|
          q.pop.should be
        end
        q.pop.should_not be
        # Finally, we should be able to find this failed job and it should have
        # failure information
        job = client.jobs.failed('failed-retries-testing')['jobs'][0]
        job.failure.should have_key('group')
        job.failure.should have_key('message')
        job.move('foo')
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
        client.config['grace-period'] = 0
        job = q.pop; job = q.pop
        job.retries_left.should eq(1)
        job.complete
        client.jobs[job.jid].retries_left.should eq(2)
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
        client.config['grace-period'] = 0
        job = q.pop; job = q.pop
        job.original_retries.should eq(2)
        job.retries_left.should eq(1)
        job.move("testing")
        client.jobs[job.jid].retries_left.should eq(2)
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
        client.workers.counts.should eq({})
        job = q.pop
        client.workers.counts.should eq([{
          "name"    => q.worker_name,
          "jobs"    => 1,
          "stalled" => 0
        }])
        client.workers[q.worker_name].should eq({
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
        client.workers.counts.should eq([{
          "name"    => q.worker_name,
          "jobs"    => 1,
          "stalled" => 0
        }])
        client.workers[q.worker_name].should eq({
          "jobs"    => [jid],
          "stalled" => {}
        })
        job.cancel
        client.workers.counts.should eq([{
          "name"    => q.worker_name,
          "jobs"    => 0,
          "stalled" => 0
        }])
        client.workers[q.worker_name].should eq({
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
        client.config['grace-period'] = 0
        job = q.pop
        client.workers.counts.should eq([{
          "name"    => q.worker_name,
          "jobs"    => 0,
          "stalled" => 1
        }])
        client.workers[q.worker_name].should eq({
          "jobs"    => {},
          "stalled" => [jid]
        })
        
        client.config["heartbeat"] = 60
        job = a.pop
        a.pop
        client.workers.counts.should eq([{
          "name"    => a.worker_name,
          "jobs"    => 1,
          "stalled" => 0
        }, {
          "name"    => q.worker_name,
          "jobs"    => 0,
          "stalled" => 0
        }])
        client.workers[q.worker_name].should eq({
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
        client.workers.counts.should eq([{
          "name"    => q.worker_name,
          "jobs"    => 1,
          "stalled" => 0
        }])
        client.workers[q.worker_name].should eq({
          "jobs"    => [jid],
          "stalled" => {}
        })
        
        # Now, let's fail it
        job.fail("foo", "bar")
        client.workers.counts.should eq([{
          "name"    => q.worker_name,
          "jobs"    => 0,
          "stalled" => 0
        }])
        client.workers[q.worker_name].should eq({
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
        client.workers.counts.should eq([{
          "name"    => q.worker_name,
          "jobs"    => 1,
          "stalled" => 0
        }])
        client.workers[q.worker_name].should eq({
          "jobs"    => [jid],
          "stalled" => {}
        })
        
        job.complete
        client.workers.counts.should eq([{
          "name"    => q.worker_name,
          "jobs"    => 0,
          "stalled" => 0
        }])
        client.workers[q.worker_name].should eq({
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
        client.workers.counts.should eq([{
          "name"    => q.worker_name,
          "jobs"    => 1,
          "stalled" => 0
        }])
        client.workers[q.worker_name].should eq({
          "jobs"    => [jid],
          "stalled" => {}
        })
        
        job.move("other")
        client.workers.counts.should eq([{
          "name"    => q.worker_name,
          "jobs"    => 0,
          "stalled" => 0
        }])
        client.workers[q.worker_name].should eq({
          "jobs"    => {},
          "stalled" => {}
        })
      end
      
      it "removes workers inactive workers after a certain amount of time" do
        # Make sure that if a worker hasn't popped any jobs in a day, that
        # it gets cleaned up after another worker pops a job
        #   1) Pop from worker a; ensure they is shows up
        #   2) Advance clock more than a day
        #   3) Check workers, make sure it's worked.
        #   4) Re-run expiriment with `max-worker-age` configuration set
        client.config['grace-period'] = 0
        Time.freeze
        jid = q.put(Qless::Job, {"test" => "workers_reput"})
        job = q.pop
        Time.advance(86300)
        client.workers.counts.should eq([{
          "name"    => q.worker_name,
          "jobs"    => 0,
          "stalled" => 1
        }])
        client.workers[q.worker_name].should eq({
          "jobs"    => {},
          "stalled" => [jid]
        })
        Time.advance(200)
        client.workers.counts.should eq({})
        client.workers[q.worker_name].should eq({
          "jobs"    => {},
          "stalled" => {}
        })
        
        client.config['max-worker-age'] = 3600
        job = q.pop
        Time.advance(3500)
        client.workers.counts.should eq([{
          "name"    => q.worker_name,
          "jobs"    => 0,
          "stalled" => 1
        }])
        client.workers[q.worker_name].should eq({
          "jobs"    => {},
          "stalled" => [jid]
        })
        Time.advance(200)
        client.workers.counts.should eq({})
        client.workers[q.worker_name].should eq({
          "jobs"    => {},
          "stalled" => {}
        })
      end

      def registered_worker_names
        client.workers.counts.map { |w| w['name'] }
      end

      it 'removes deregistered workers' do
        q.put(Qless::Job, {"test" => "workers_reput"})
        q.pop

        expect {
          client.deregister_workers(q.worker_name)
        }.to change { registered_worker_names }.from([q.worker_name]).to([])
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
        (q.jobs.stalled(0, 10) + q.jobs.stalled(10, 10)).to_set.should eq(jids)
        
        client.config["heartbeat"] = 60
        jobs = q.pop(20)
        (q.jobs.running(0, 10) + q.jobs.running(10, 10)).to_set.should eq(jids)
        
        jids = 20.times.map { |i| q.put(Qless::Job, {"test" => "rssd"}, :delay => 60) }
        (q.jobs.scheduled(0, 10) + q.jobs.scheduled(10, 10)).to_set.should eq(jids.to_set)
        
        jids = 20.times.map { |i| q.put(Qless::Job, {"test" => "rssd"}, :depends => jids) }
        (q.jobs.depends(0, 10) + q.jobs.depends(10, 10)).to_set.should eq(jids.to_set)
      end

      it "does not include scheduled jobs whose time has now come in the scheduled list" do
        Time.freeze

        jid = q.put(Qless::Job, {}, delay: 10)
        expect(q.peek).to be_nil
        expect(q.jobs.scheduled).to eq([jid])

        Time.advance(11)
        expect(q.peek).to be_a(Qless::Job)
        expect(q.jobs.scheduled).to eq([])
      end
    end

    describe "#log" do
      # We should be able to add log messages to jobs that exist
      it "can add logs to existing jobs" do
        jid = q.put(Qless::Job, {})
        client.jobs[jid].log('Something', {:foo => 'bar'})
        history = client.jobs[jid].history
        history.length.should eq(2)
        history[1]['what'].should eq('Something')
        history[1]['foo'].should eq('bar')

        # The data part should be optional
        client.jobs[jid].log('Foo')
        history = client.jobs[jid].history
        history.length.should eq(3)
        history[2]['what'].should eq('Foo')
      end

      # If a job doesn't exist, it throws an error
      it "throws an error if that job doesn't exist" do
        job = client.jobs[q.put(Qless::Job, {})]
        job.cancel
        expect {
          job.log('foo')
        }.to raise_error(/does not exist/)
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
        job.original_retries.should eq(job.retries_left)
        job.retry()
        # Pop is off again
        q.jobs.scheduled().should eq([])
        client.jobs[job.jid].state.should eq('waiting')
        job = q.pop
        job.should_not eq(nil)
        job.original_retries.should eq(job.retries_left + 1)
        # Retry it again, with a backoff
        job.retry(60)
        q.pop.should eq(nil)
        q.jobs.scheduled.should eq([jid])
        job = client.jobs[jid]
        job.original_retries.should eq(job.retries_left + 2)
        job.state.should eq('scheduled')
      end
      
      it "fails when we exhaust its retries through retry()" do
        jid = q.put(Qless::Job, {'test' => 'test_retry_fail'}, :retries => 2)
        client.jobs.failed.should eq({})
        q.pop.retry.should eq(1)
        q.pop.retry.should eq(0)
        q.pop.retry.should eq(-1)
        client.jobs.failed.should eq({'failed-retries-testing' => 1})
      end
      
      it "prevents us from retrying jobs not running" do
        job = client.jobs[q.put(Qless::Job, {'test' => 'test_retry_error'})]
        expect {
          job.retry
        }.to raise_error(Qless::LuaScriptError, /not currently running/)
        q.pop.fail('foo', 'bar')
        expect {
          job.retry
        }.to raise_error(Qless::LuaScriptError, /not currently running/)
        client.jobs[job.jid].move('testing')
        job = q.pop;
        job.instance_variable_set(:@worker_name, 'foobar')
        expect {
          job.retry
        }.to raise_error(Qless::LuaScriptError, /handed out to another/)
        job.instance_variable_set(:@worker_name, Qless.worker_name)
        job.complete
        expect {
          job.retry
        }.to raise_error(Qless::LuaScriptError, /not currently running/)
      end
      
      it "stops reporting a job as being associated with a worker when is retried" do
        jid = q.put(Qless::Job, {'test' => 'test_retry_workers'})
        job = q.pop
        client.workers[Qless.worker_name].should eq({'jobs' => [jid], 'stalled' => {}})
        job.retry.should eq(4)
        client.workers[Qless.worker_name].should eq({'jobs' => {}, 'stalled' => {}})
      end

      it "accepts a group and message" do
        # If desired, we can provide a group and message just like we would if
        # there were a real failure. If we do, then we should be able to see it
        # in the job
        jid = q.put(Qless::Job, {}, :retries => 0)
        job = q.pop
        job.retry(0, 'foo', 'bar')
        job = client.jobs[jid]
        job.state.should eq('failed')
        job.failure['group'].should eq('foo')
        job.failure['message'].should eq('bar')
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
        a = q.put(Qless::Job, {"test" => "priority"}, :priority => 10)
        b = q.put(Qless::Job, {"test" => "priority"})
        q.peek.jid.should eq(a)
        client.jobs[b].priority = 20
        q.length.should eq(2)
        q.peek.jid.should eq(b)
        job = q.pop
        q.length.should eq(2)
        job.jid.should eq(b)
        job = q.pop
        q.length.should eq(2)
        job.jid.should eq(a)
        job.priority = 30
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
        job = client.jobs[q.put(Qless::Job, {"test" => "tag"})]
        client.jobs.tagged('foo').should eq({"total" => 0, "jobs" => {}})
        client.jobs.tagged('bar').should eq({"total" => 0, "jobs" => {}})
        job.tag('foo')
        client.jobs.tagged('foo').should eq({"total" => 1, "jobs" => [job.jid]})
        client.jobs.tagged('bar').should eq({"total" => 0, "jobs" => {}})
        job.tag('bar')
        client.jobs.tagged('foo').should eq({"total" => 1, "jobs" => [job.jid]})
        client.jobs.tagged('bar').should eq({"total" => 1, "jobs" => [job.jid]})
        job.untag('foo')
        client.jobs.tagged('foo').should eq({"total" => 0, "jobs" => {}})
        client.jobs.tagged('bar').should eq({"total" => 1, "jobs" => [job.jid]})
        job.untag('bar')
        client.jobs.tagged('foo').should eq({"total" => 0, "jobs" => {}})
        client.jobs.tagged('bar').should eq({"total" => 0, "jobs" => {}})
      end
      
      it "can preserve the order of tags" do
        job = client.jobs[q.put(Qless::Job, {"test" => "preserve_order"})]
        tags = %w{a b c d e f g h}
        tags.length.times do |i|
          job.tag(tags[i])
          client.jobs[job.jid].tags.should eq(tags[0..i])
        end
        
        # Now let's take a few out
        job.untag('a', 'c', 'e', 'g')
        client.jobs[job.jid].tags.should eq(%w{b d f h})
      end
      
      it "removes tags when canceling / expiring jobs" do
        job = client.jobs[q.put(Qless::Job, {"test" => "cancel_expire"})]
        job.tag("foo", "bar")
        client.jobs.tagged('foo').should eq({"total" => 1, "jobs" => [job.jid]})
        client.jobs.tagged('bar').should eq({"total" => 1, "jobs" => [job.jid]})
        job.cancel()
        client.jobs.tagged('foo').should eq({"total" => 0, "jobs" => {}})
        client.jobs.tagged('bar').should eq({"total" => 0, "jobs" => {}})
        
        # Now we have job expire from completion
        client.config['jobs-history-count'] = 0
        q.put(Qless::Job, {"test" => "cancel_expire"})
        job = q.pop
        job.should_not eq(nil)
        job.tag('foo', 'bar')
        client.jobs.tagged('foo').should eq({"total" => 1, "jobs" => [job.jid]})
        client.jobs.tagged('bar').should eq({"total" => 1, "jobs" => [job.jid]})
        job.complete
        client.jobs.tagged('foo').should eq({"total" => 0, "jobs" => {}})
        client.jobs.tagged('bar').should eq({"total" => 0, "jobs" => {}})
        
        # If the job no longer exists, attempts to tag it should not add to
        # the set
        expect {
          job.tag('foo', 'bar')
        }.to raise_error(/does not exist/)
        client.jobs.tagged('foo').should eq({"total" => 0, "jobs" => {}})
        client.jobs.tagged('bar').should eq({"total" => 0, "jobs" => {}})
      end
      
      it "can tag a job when we initially put it on" do
        client.jobs.tagged('foo').should eq({"total" => 0, "jobs" => {}})
        client.jobs.tagged('bar').should eq({"total" => 0, "jobs" => {}})
        jid = q.put(Qless::Job, {'test' => 'tag_put'}, :tags => ['foo', 'bar'])
        client.jobs.tagged('foo').should eq({"total" => 1, "jobs" => [jid]})
        client.jobs.tagged('bar').should eq({"total" => 1, "jobs" => [jid]})
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
          client.jobs[jid].cancel
        end
        # Add only one back
        q.put(Qless::Job, {}, :tags => ['foo'])
        client.tags.should eq({})
        # Add a second, and tag it
        b = client.jobs[q.put(Qless::Job, {})]
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
        client.jobs[jid].state.should eq('depends')
        job.complete
        client.jobs[jid].state.should eq('waiting')
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
        client.jobs[a].state.should eq('depends')
        jobs = q.pop(20)
        jobs.length.should eq(1)
        jobs[0].complete
        client.jobs[a].state.should eq('waiting')
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
        client.jobs[b].cancel
        job = q.pop
        job.jid.should eq(a)
        job.complete.should eq('complete')
        q.pop.should eq(nil)
        
        a = q.put(Qless::Job, {"test" => "depends_canceled"})
        b = q.put(Qless::Job, {"test" => "depends_canceled"}, :depends => [a])
        expect {
          client.jobs[a].cancel
        }.to raise_error(/is a dependency/)
      end

      def create_dep_graph
        a = q.put(Qless::Job, {"test" => "depends_canceled"})
        b = q.put(Qless::Job, {"test" => "depends_canceled"}, :depends => [a])

        return a, b
      end

      it 'can bulk cancel a dependency graph of jobs, regardless of the ordering of the jids' do
        jids = create_dep_graph
        expect { client.bulk_cancel(jids) }.to change { q.length }.to(0)

        jids = create_dep_graph
        expect { client.bulk_cancel(jids.reverse) }.to change { q.length }.to(0)
      end

      it 'cannot bulk cancel a set of jids that have dependencies outside the jid set' do
        a = q.put(Qless::Job, {"test" => "depends_canceled"})
        b = q.put(Qless::Job, {"test" => "depends_canceled"}, :depends => [a])
        c = q.put(Qless::Job, {"test" => "depends_canceled"})
        d = q.put(Qless::Job, {"test" => "depends_canceled"}, :depends => [c, a])

        expect { client.bulk_cancel([a, b, c]) }.to raise_error(/is a dependency/)
      end


      it 'ignores unknown jids given to bulk_cancel as they may represent previously cancelled jobs' do
        jid_1, jid_2 = create_dep_graph
        jids = ["not_a_real_jid_1", jid_1, "not_a_real_jid_2", jid_2, "not_a_real_jid_3"]

        expect { client.bulk_cancel(jids) }.to change { q.length }.to(0)
        expect(client.jobs[jid_1]).to be_nil
        expect(client.jobs[jid_2]).to be_nil
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
        client.jobs[b].move("other")
        client.jobs[b].state.should eq("depends")
        other.pop.should eq(nil)
        q.pop.complete
        client.jobs[b].state.should eq("waiting")
        other.pop.jid.should eq(b)
      end
      
      it "supports adding dependencies" do
        # If we have a job that already depends on on other jobs, then
        # we should be able to add more dependencies. If it's not, then
        # we can't
        a = q.put(Qless::Job, {"test" => "add_dependency"})
        b = q.put(Qless::Job, {"test" => "add_dependency"}, :depends => [a])
        c = q.put(Qless::Job, {"test" => "add_dependency"})
        client.jobs[b].depend(c).should eq(true)
        
        jobs = q.pop(20)
        jobs.length.should eq(2)
        jobs[0].jid.should eq(a)
        jobs[1].jid.should eq(c)
        jobs[0].complete
        q.pop.should eq(nil)
        jobs[1].complete
        q.pop.jid.should eq(b)
      end
      
      it "supports removing dependencies" do
        # If we have a job that already depends on others, then we should
        # we able to remove them. If it's not dependent on any, then we can't.        
        a = q.put(Qless::Job, {"test" => "remove_dependency"})
        b = q.put(Qless::Job, {"test" => "remove_dependency"}, :depends => [a])
        q.pop(20).length.should eq(1)
        client.jobs[b].undepend(a)
        q.pop.jid.should eq(b)
        
        # Let's try removing /all/ dependencies
        jids = 10.times.map { |i| q.put(Qless::Job, {"test" => "remove_dependency"}) }
        b = q.put(Qless::Job, {"test" => "remove_dependency"}, :depends => jids)
        q.pop(20).length.should eq(10)
        client.jobs[b].undepend(:all)
        client.jobs[b].state.should eq('waiting')
        q.pop.jid.should eq(b)
        jids.each do |jid|
          client.jobs[jid].dependents.should eq([])
        end
        
        # Job must be in the 'depends' state to manipulate dependencies
        a = q.put(Qless::Job, {"test" => "remove_dependency"})
        b = q.put(Qless::Job, {"test" => "remove_dependency"})
        expect {
          client.jobs[a].undepend(b)
        }.to raise_error(/not in the depends state/)
        job = q.pop
        expect {
          job.undepend(b)
        }.to raise_error(/not in the depends state/)

        job.fail('what', 'something')
        expect {
          client.jobs[job.jid].undepend(b)
        }.to raise_error(/not in the depends state/)
      end
      
      it "lets us see dependent jobs in a queue" do
        # When we have jobs that have dependencies, we should be able to
        # get access to them.
        a = q.put(Qless::Job, {"test" => "jobs_depends"})
        b = q.put(Qless::Job, {"test" => "jobs_depends"}, :depends => [a])
        client.queues.counts[0]['depends'].should eq(1)
        client.queues['testing'].counts['depends'].should eq(1)
        q.jobs.depends().should eq([b])
        
        # When we remove a dependency, we should no longer see that job as a dependency
        client.jobs[b].undepend(a)
        client.queues.counts[0]['depends'].should eq(0)
        client.queues['testing'].counts['depends'].should eq(0)
        q.jobs.depends().should eq([])
        
        # When we move a job that has a dependency, we should no longer
        # see it in the depends() of the original job
        a = q.put(Qless::Job, {"test" => "jobs_depends"})
        b = q.put(Qless::Job, {"test" => "jobs_depends"}, :depends => [a])
        client.queues.counts[0]['depends'].should eq(1)
        client.queues['testing'].counts['depends'].should eq(1)
        q.jobs.depends().should eq([b])
        
        # When we remove a dependency, we should no longer see that job as a dependency
        client.jobs[b].move('other')
        client.queues.counts[0]['depends'].should eq(0)
        client.queues['testing'].counts['depends'].should eq(0)
        q.jobs.depends().should eq([])
      end
    end

    describe "#pause" do
      it 'stops the given queue from being processed until #unpause is called' do
        pausable_queue = client.queues["pausable"]
        other_queue = client.queues["other"]

        pausable_queue.put(Qless::Job, {})
        other_queue.put(Qless::Job, {})

        pausable_queue.pause

        3.times do
          pausable_queue.pop.should be(nil)
          pausable_queue.peek.should_not be(nil)
        end

        other_queue.peek.should_not be(nil)
        other_queue.pop.should_not be(nil)

        pausable_queue.unpause

        pausable_queue.peek.should_not be(nil)
        pausable_queue.pop.should_not be(nil)
      end

      it 'can optionally stop all running jobs' do
        queue = client.queues['pausable']
        10.times.map { |i| queue.put(Qless::Job, {}, :jid => i) }
        queue.pop(5)

        # Now, we'll make sure that they're marked 'running' after a pause
        queue.pause
        counts = client.queues.counts
        counts.length.should eq(1)
        counts[0]['running'].should eq(5)

        # Now we'll unpause it and repause it but with a 'stopjobs' option
        queue.unpause
        queue.pause(:stopjobs => true)
        counts = client.queues.counts
        counts.length.should eq(1)
        counts[0]['running'].should eq(0)
      end
    end

    describe "#grace-period" do
      it "doesn't immediately hand out a job" do
        client.config['heartbeat'] = -10
        jid = q.put(Qless::Job, {}, :retries => 5)
        ajob = q.pop
        bjob = q.pop
        bjob.should_not be
        ajob.retry(0, 'foo', 'bar').should eq(4)

        # Now, the job should have failure information, and we can pop it
        client.jobs[jid].failure['group'].should eq('foo')
        bjob = q.pop
        bjob.should be

        # We should see that when it does eventually fail, that it doesn't
        # replace the group, message
        4.times do |i|
          bjob.retry(0, 'foo', 'bar')
          bjob = q.pop
          bjob.retries_left.should eq(4 - i - 1)
        end

        bjob.retry(0, 'foo', 'bar')
        client.jobs[jid].failure['group'].should eq('foo')
        client.jobs[jid].state.should eq('failed')
      end
    end
    
    describe "#lua" do
      it "checks complete's arguments" do
        lua_helper('complete', [
          # Not enough args
          [],
          # Missing worker
          ['deadbeef'],
          # Missing queue
          ['deadbeef', 'worker1'],
          # Malformed JSON
          ['deadbeef', 'worker1', 'foo', '[}'],
          # Not a number for delay
          ['deadbeef', 'worker1', 'foo', '{}', 'next', 'howdy', 'delay',
              'howdy'],
          # Mutually exclusive options
          ['deadbeef', 'worker1', 'foo', '{}', 'next', 'foo', 'delay', 5,
              'depends', '["foo"]'],
          # Mutually inclusive options (with 'next')
          ['deadbeef', 'worker1', 'foo', '{}', 'delay', 5],
          ['deadbeef', 'worker1', 'foo', '{}', 'depends', '["foo"]']
        ])
      end
      
      it "checks fail's arguments" do
        lua_helper('fail', [
          # Missing id
          [],
          # Missing worker
          ['deadbeef'],
          # Missing type
          ['deadbeef', 'worker1'],
          # Missing message
          ['deadbeef', 'worker1', 'foo'],
          # Malformed data
          ['deadbeef', 'worker1', 'foo', 'bar', '[}']
        ])
      end
      
      it "checks failed's arguments" do
        lua_helper('failed', [
          # Malformed start
          ['bar', 'howdy'],
          # Malformed limit
          ['bar', 0, 'howdy'],
        ])
      end
      
      it "checks get's arguments" do
        lua_helper('get', [
          # Missing jid
          []
        ])
      end
      
      it "checks heartbeat's arguments" do
        lua_helper('heartbeat', [
          # Missing id
          [],
          # Missing worker
          ['deadbeef'],
          # Missing expiration
          ['deadbeef', 'worker1'],
          # Malformed JSON
          ['deadbeef', 'worker1', '[}']
        ])
      end
      
      it "checks jobs' arguments" do
        lua_helper('jobs', [
          # Unrecognized option
          ['testing'],
          # Missing queue
          ['stalled']
        ])
      end
      
      it "checks peek's arguments" do
        lua_helper('peek', [
          # Missing count
          [],
          # Malformed count
          ['howdy']
        ])
      end
      
      it "checks pop's arguments" do
        lua_helper('pop', [
          # Missing worker
          [],
          # Missing count
          ['worker1'],
          # Malformed count
          ['worker1', 'howdy'],
        ])
      end
      
      it "checks priority's arguments" do
        lua_helper('priority', [
          # Missing jid
          [],
          # Missing priority
          ['12345'],
          # Malformed priority
          ['12345', 'howdy'],
        ])
      end
      
      it "checks put's arguments" do
        lua_helper('put', [
          # Missing id
          [],
          # Missing klass
          ['deadbeef'],
          # Missing data
          ['deadbeef', 'foo'],
          # Malformed data
          ['deadbeef', 'foo', '[}'],
          # Non-dictionary data
          ['deadbeef', 'foo', '[]'],
          # Non-dictionary data
          ['deadbeef', 'foo', '"foobar"'],
          # Malformed delay
          ['deadbeef', 'foo', '{}', 'howdy'],
          # Malformed priority
          ['deadbeef', 'foo', '{}', 0, 'priority', 'howdy'],
          # Malformed tags
          ['deadbeef', 'foo', '{}', 0, 'tags', '[}'],
          # Malformed retries
          ['deadbeef', 'foo', '{}', 0, 'retries', 'hello'],
          # Mutually exclusive options
          ['deadbeef', 'foo', '{}', 5, 'depends', '["hello"]']
        ])
      end
      
      it "checks recur's arguments" do
        lua_helper('recur.on', [
          [],
          ['testing'],
          ['testing', 'foo.klass'],
          ['testing', 'foo.klass', '{}'],
          ['testing', 'foo.klass', '{}', 12345],
          ['testing', 'foo.klass', '{}', 12345, 'interval'],
          ['testing', 'foo.klass', '{}', 12345, 'interval', 12345],
          ['testing', 'foo.klass', '{}', 12345, 'interval', 12345, 0],
          # Malformed data, priority, tags, retries
          ['testing', 'foo.klass', '[}', 12345, 'interval', 12345, 0],
          ['testing', 'foo.klass', '{}', 12345, 'interval', 12345, 0,
              'priority', 'foo'],
          ['testing', 'foo.klass', '{}', 12345, 'interval', 12345, 0,
              'retries', 'foo'],
          ['testing', 'foo.klass', '{}', 12345, 'interval', 12345, 0,
              'tags', '[}'],
        ])

        lua_helper('recur.off', [
          # Missing jid
          []
        ])

        lua_helper('recur.get', [
          # Missing jid
          []
        ])

        lua_helper('recur.update', [
          ['update'],
          ['update', 'priority', 'foo'],
          ['update', 'interval', 'foo'],
          ['update', 'retries', 'foo'],
          ['update', 'data', '[}'],
        ])
      end
      
      it "checks retry's arguments" do
        lua_helper('retry', [
          # Missing queue
          ['12345'],
          # Missing worker
          ['12345', 'testing'],
          # Malformed delay
          ['12345', 'testing', 'worker', 'howdy'],
        ])
      end
            
      it "checks stats' arguments" do
        lua_helper('stats', [
          # Missing queue
          [],
          # Missing date
          ['foo']
        ])
      end
      
      it "checks tags' arguments" do
        lua_helper('tag', [
          # Missing jid
          ['add'],
          # Missing jid
          ['remove'],
          # Missing tag
          ['get'],
          # Malformed offset
          ['get', 'foo', 'howdy'],
          # Malformed count
          ['get', 'foo', 0, 'howdy']
        ])
      end
      
      it "checks track's arguments" do
        lua_helper('track', [
          # Unknown command
          ['fslkdjf', 'deadbeef'],
          # Missing jid
          ['track']
        ])
      end
    end
  end
end
