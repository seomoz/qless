ENV['RACK_ENV'] = 'test'
require 'spec_helper'
require 'yaml'
require 'qless'
require 'qless/server'
require 'capybara/rspec'

module Qless
  describe Server, :integration, :type => :request do
    # Our main test queue
    let(:q)      { client.queue("testing") }
    # Point to the main queue, but identify as different workers
    let(:a)      { client.queue("testing").tap { |o| o.worker_name = "worker-a" } }
    let(:b)      { client.queue("testing").tap { |o| o.worker_name = "worker-b" } }
    # And a second queue
    let(:other)  { client.queue("other")   }
    
    before(:all) do
      Qless::Server.client = Qless::Client.new(redis_config)
      Capybara.app = Qless::Server.new
    end

    it 'can visit each top-nav tab' do
      visit '/'

      links = all('ul.nav a')
      links.should have_at_least(6).links
      links.each do |link|
        click_link link.text
      end
    end
    
    it 'can see the root-level summary' do
      visit '/'
      
      # When qless is empty, we should see the default bit
      first('h1', :text => /no queues/i).should be
      first('h1', :text => /no failed jobs/i).should be
      first('h1', :text => /no workers/i).should be
      
      # Alright, let's add a job to a queue, and make sure we can see it
      jid = q.put(Qless::Job, {})
      visit '/'
      first('h3', :text => /testing/).should be
      first('h3', :text => /0\D+1\D+0\D+0\D+0/).should be
      first('h1', :text => /no queues/i).should be_nil
      first('h1', :text => /queues and their job counts/i).should be
      
      # Let's pop the job, and make sure that we can see /that/
      job = q.pop
      visit '/'
      first('h3', :text => /1\D+0\D+0\D+0\D+0/).should be
      first('h3', :text => q.worker_name).should be
      first('h3', :text => /1\D+0\D+running\W+stalled/i).should be
      
      # Let's complete the job, and make sure it disappears
      job.complete
      visit '/'
      first('h3', :text => /0\D+0\D+0\D+0\D+0/).should be
      first('h3', :text => /0\D+0\D+running\W+stalled/i).should be
      
      # Let's put and pop and fail a job, and make sure we see it
      jid = q.put(Qless::Job, {})
      job = q.pop
      job.fail('foo-failure', 'bar')
      visit '/'
      first('h3', :text => /foo-failure/).should be
      first('h3', :text => /1\D+jobs/i).should be
      
      # And let's have one scheduled, and make sure it shows up accordingly
      jid = q.put(Qless::Job, {}, :delay => 60)
      visit '/'
      first('h3', :text => /0\D+0\D+1\D+0\D+0/).should be
      # And one that depends on that job
      jid = q.put(Qless::Job, {}, :depends => [jid])
      visit '/'
      first('h3', :text => /0\D+0\D+1\D+0\D+1/).should be
    end
    
    it 'can visit the tracked page' do
      # We should be able to see all of the appropriate tabs,
      # We should be able to see all of the jobs
      jid = q.put(Qless::Job, {})
      client.job(jid).track()
      
      visit '/track'
      # Make sure it appears under 'all', and 'waiting'
      first('a', :text => /all\W+1/i).should be
      first('a', :text => /waiting\W+1/i).should be
      # Now let's pop off the job so that it's running
      job = q.pop
      visit '/track'
      first('a', :text => /all\W+1/i).should be
      first('a', :text => /waiting\W+0/i).should be
      first('a', :text => /running\W+1/i).should be
      # Now let's complete the job and make sure it shows up again
      job.complete
      visit '/track'
      first('a', :text => /all\W+1/i).should be
      first('a', :text => /running\W+0/i).should be
      first('a', :text => /completed\W+1/i).should be
      job.untrack
      
      # And now for a scheduled job
      job = client.job(q.put(Qless::Job, {}, :delay => 600))
      job.track
      visit '/track'
      first('a', :text => /all\W+1/i).should be
      first('a', :text => /scheduled\W+1/i).should be
      job.untrack
      
      # And finally a failed job
      q.put(Qless::Job, {}); job = q.pop
      job.track
      job.fail('foo', 'bar')
      visit '/track'
      first('a', :text => /all\W+1/i).should be
      first('a', :text => /failed\W+1/i).should be
      job.untrack
    end
    
    it 'can display the correct buttons for jobs' do
      # Depending on the state of the job, it can display
      # the appropriate buttons. In particular...
      #    - A job always gets the 'move' dropdown
      #    - A job always gets the 'track' flag
      #    - A failed job gets the 'retry' button
      #    - An incomplete job gets the 'cancel' button
      job = client.job(q.put(Qless::Job, {}))
      visit "/jobs/#{job.jid}"
      first('i.icon-remove').should be
      first('i.icon-repeat').should be_nil
      first('i.icon-flag'  ).should be
      first('span.caret'   ).should be
      
      # Let's fail the job and that it has the repeat button
      q.pop.fail('foo', 'bar')
      visit "/jobs/#{job.jid}"
      first('i.icon-remove').should be
      first('i.icon-repeat').should be
      first('i.icon-flag'  ).should be
      first('span.caret'   ).should be
      
      # Now let's complete the job and see that it doesn't have
      # the cancel button
      job.move('testing')
      q.pop.complete
      visit "/jobs/#{job.jid}"
      first('i.icon-remove').should be_nil
      first('i.icon-repeat').should be_nil
      first('i.icon-flag'  ).should be
      first('span.caret'   ).should be
    end
    
    it 'can display tags and priorities for jobs' do
      visit "/jobs/#{q.put(Qless::Job, {})}"
      first('span', :text => /\+\D*0/).should be
      
      visit "/jobs/#{q.put(Qless::Job, {}, :priority => 123)}"
      first('span', :text => /\+\D*123/).should be
      
      visit "/jobs/#{q.put(Qless::Job, {}, :priority => -123)}"
      first('span', :text => /\-\D*123/).should be
      
      visit "/jobs/#{q.put(Qless::Job, {}, :tags => ['foo', 'bar', 'widget'])}"
      %w{foo bar widget}.each do |tag|
        first('span', :text => tag).should be
      end
    end
    
    it 'can track a job', :js => true do
      # Make sure the job doesn't appear as tracked first, then 
      # click the 'track' button, and come back to verify that
      # it's now being tracked
      jid = q.put(Qless::Job, {})
      visit '/track'
      first('a', :text => /#{jid[0..5]}/i).should be_nil
      visit "/jobs/#{jid}"
      first('i.icon-flag').click
      visit '/track'
      first('a', :text => /#{jid[0..5]}/i).should be
    end
    
    it 'can move a job', :js => true do
      # Let's put a job, pop it, complete it, and then move it
      # back into the testing queue
      jid = q.put(Qless::Job, {})
      q.pop.complete
      visit "/jobs/#{jid}"
      # Get the job, check that it's complete
      client.job(jid).state.should eq('complete')
      first('span.caret').click
      first('a', :text => 'testing').click
      # Now get the job again, check it's waiting
      client.job(jid).state.should eq('waiting')
      client.job(jid).queue.should eq('testing')
    end
    
    it 'can retry a single job', :js => true do
      # Put, pop, and fail a job, and then click the retry button
      jid = q.put(Qless::Job, {})
      q.pop.fail('foo', 'bar')
      visit "/jobs/#{jid}"
      # Get the job, check that it's failed
      client.job(jid).state.should eq('failed')
      first('i.icon-repeat').click
      # Now get hte jobs again, check that it's waiting
      client.job(jid).state.should eq('waiting')
      client.job(jid).queue.should eq('testing')
    end
    
    it 'can cancel a single job', :js => true do
      # Put, pop, and fail a job, and then click the retry button
      jid = q.put(Qless::Job, {})
      visit "/jobs/#{jid}"
      # Get the job, check that it's failed
      client.job(jid).state.should eq('waiting')
      first('button.btn-danger').click
      # We should have to click the cancel button now
      client.job(jid).should be
      first('button.btn-danger').click
      # /Now/ the job should be canceled
      client.job(jid).should be_nil
    end
    
    it 'can visit the configuration' do
      # Visit the bare-bones config page, make sure defaults are
      # present, then set some configurations, and then make sure
      # they appear as well
      visit '/config'
      first('h2', :text => /jobs-history-count/i).should be
      first('h2', :text => /stats-history/i).should be
      first('h2', :text => /jobs-history/i).should be
      
      client.config['foo-bar'] = 50
      visit '/config'
      first('h2', :text => /jobs-history-count/i).should be
      first('h2', :text => /stats-history/i).should be
      first('h2', :text => /jobs-history/i).should be
      first('h2', :text => /foo-bar/i).should be
    end
    
    it 'can search by tag' do
      # We should tag some jobs, and then search by tags and ensure
      # that we find all the jobs we'd expect
      foo    = 5.times.map { |i| q.put(Qless::Job, {}, :tags => ['foo']) }
      bar    = 5.times.map { |i| q.put(Qless::Job, {}, :tags => ['bar']) }
      foobar = 5.times.map { |i| q.put(Qless::Job, {}, :tags => ['foo', 'bar']) }
      
      visit '/tag?tag=foo'
      (foo + foobar).each do |jid|
        first('a', :text => /#{jid[0..5]}/i).should be
      end
      
      visit '/tag?tag=bar'
      (bar + foobar).each do |jid|
        first('a', :text => /#{jid[0..5]}/i).should be
      end
    end
    
    it 'can visit the page for a specific job' do
      # We should make sure we see details like its state, the queue
      # that it's in, its data, and any failure information
      jid = q.put(Qless::Job, {'foo' => 'bar'})
      job = client.job(jid)
      visit "/jobs/#{jid}"
      # Make sure we see its klass_name, queue, state and data
      first('h2', :text => /#{job.klass}/i).should be
      first('h2', :text => /#{job.queue}/i).should be
      first('h2', :text => /#{job.state}/i).should be
      first('pre', :text => /\"foo\"\s*:\s*\"bar\"/im).should be
      
      # Now let's pop the job and fail it just to make sure we can see the error
      q.pop.fail('something-something', 'what-what')
      visit "/jobs/#{jid}"
      first('pre', :text => /what-what/im).should be
    end
    
    it 'can visit the failed page' do
      # We should make sure that we see all the groups of failures that
      # we expect, as well as all the jobs we'd expect. This includes the
      # tabs, but also the section headings
      visit '/failed'
      first('li', :text => /foo/i).should be_nil
      first('li', :text => /bar/i).should be_nil
      
      foo = 5.times.map { |i| q.put(Qless::Job, {}) }
      q.pop(5).each { |job| job.fail('foo', 'foo-messae') }
      bar = 5.times.map { |i| q.put(Qless::Job, {}) }
      q.pop(5).each { |job| job.fail('bar', 'bar-messae') }
      visit '/failed'
      first('li', :text => /foo\D+5/i).should be
      first('li', :text => /bar\D+5/i).should be
      first('h2', :text => /foo\D+5/i).should be
      first('h2', :text => /bar\D+5/i).should be
      (foo + bar).each do |jid|
        first('a', :text => /#{jid[0..5]}/i).should be
      end
    end
    
    it 'can retry a group of failed jobs', :js => true do
      # We'll fail a bunch of jobs, with two kinds of errors,
      # and then we'll make sure that we can retry all of 
      # one kind, but the rest still remain failed.
      foo = 5.times.map { |i| q.put(Qless::Job, {}) }
      q.pop(5).each { |job| job.fail('foo', 'foo-messae') }
      bar = 5.times.map { |i| q.put(Qless::Job, {}) }
      q.pop(5).each { |job| job.fail('bar', 'bar-messae') }
      
      visit '/failed'
      first('li', :text => /foo\D+5/i).should be
      first('h2', :text => /foo\D+5/i).should be
      first('li', :text => /bar\D+5/i).should be
      first('h2', :text => /bar\D+5/i).should be
      (foo + bar).each do |jid|
        first('a', :text => /#{jid[0..5]}/i).should be
      end
      
      retry_button = (all('button').select do |b|
        not b['onclick'].nil? and b['onclick'].include?('retryall') and b['onclick'].include?('foo')
      end).first
      retry_button.should be
      retry_button.click
      
      # Now we shouldn't see any of those jobs, but we should
      # still see bar jobs
      visit '/failed'
      first('li', :text => /foo\D+5/i).should be_nil
      first('h2', :text => /foo\D+5/i).should be_nil
      first('li', :text => /bar\D+5/i).should be
      first('h2', :text => /bar\D+5/i).should be
      bar.each do |jid|
        first('a', :text => /#{jid[0..5]}/i).should be
      end
      foo.each do |jid|
        first('a', :text => /#{jid[0..5]}/i).should be_nil
        client.job(jid).state.should eq('waiting')
      end
    end
    
    it 'can cancel a group of failed jobs', :js => true do
      # We'll fail a bunch of jobs, with two kinds of errors,
      # and then we'll make sure that we can retry all of 
      # one kind, but the rest still remain failed.
      foo = 5.times.map { |i| q.put(Qless::Job, {}) }
      q.pop(5).each { |job| job.fail('foo', 'foo-messae') }
      bar = 5.times.map { |i| q.put(Qless::Job, {}) }
      q.pop(5).each { |job| job.fail('bar', 'bar-messae') }
      
      visit '/failed'
      first('li', :text => /foo\D+5/i).should be
      first('h2', :text => /foo\D+5/i).should be
      first('li', :text => /bar\D+5/i).should be
      first('h2', :text => /bar\D+5/i).should be
      (foo + bar).each do |jid|
        first('a', :text => /#{jid[0..5]}/i).should be
      end
      
      retry_button = (all('button').select do |b|
        not b['onclick'].nil? and b['onclick'].include?('cancelall') and b['onclick'].include?('foo')
      end).first
      retry_button.should be
      retry_button.click
      # One click ain't gonna cut it
      foo.each do |jid|
        client.job(jid).state.should eq('failed')
      end
      retry_button.click
      
      # Now we shouldn't see any of those jobs, but we should
      # still see bar jobs
      visit '/failed'
      first('li', :text => /foo\D+5/i).should be_nil
      first('h2', :text => /foo\D+5/i).should be_nil
      first('li', :text => /bar\D+5/i).should be
      first('h2', :text => /bar\D+5/i).should be
      bar.each do |jid|
        first('a', :text => /#{jid[0..5]}/i).should be
      end
      foo.each do |jid|
        first('a', :text => /#{jid[0..5]}/i).should be_nil
        client.job(jid).should be_nil
      end
    end
  end
end