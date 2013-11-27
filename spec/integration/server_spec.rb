# Encoding: utf-8

ENV['RACK_ENV'] = 'test'
require 'spec_helper'
require 'yaml'
require 'qless'
require 'qless/server'
require 'capybara/rspec'
require 'capybara/poltergeist'
require 'rack/test'

Capybara.javascript_driver = :poltergeist

module Qless
  describe Server, :integration, type: :request do
    # Our main test queue
    let(:q) { client.queues['testing'] }
    # Point to the main queue, but identify as different workers
    let(:a) { client.queues['testing'].tap { |o| o.worker_name = 'worker-a' } }
    let(:b) { client.queues['testing'].tap { |o| o.worker_name = 'worker-b' } }
    # And a second queue
    let(:other)  { client.queues['other']   }

    before(:all) do
      Capybara.app = Qless::Server.new(Qless::Client.new(redis_config))
    end

    # Ensure the phantomjs process doesn't live past these tests.
    # Otherwise, they are additional child processes that interfere
    # with the tests for the forking server, since it uses
    # `wait2` to wait on any child process.
    after(:all) do
      Capybara.using_driver(:poltergeist) do
        Capybara.current_session.driver.quit
      end
    end

    it 'can visit each top-nav tab' do
      visit '/'

      links = all('ul.nav a')
      links.should have_at_least(7).links
      links.each do |link|
        click_link link.text
      end
    end

    def build_paginated_objects
      # build 30 since our page size is 25 so we have at least 2 pages
      30.times do |i|
        yield "jid-#{i + 1}"
      end
    end

    # We put periods on the end of these jids so that
    # an assertion about "jid-1" will not pass if "jid-11"
    # is on the page. The jids are formatted as "#{jid}..."
    def assert_page(present_jid_num, absent_jid_num)
      page.should have_content("jid-#{present_jid_num}.")
      page.should_not have_content("jid-#{absent_jid_num}.")
    end

    def click_pagination_link(text)
      within '.pagination' do
        click_link text
      end
    end

    def test_pagination(page_1_jid = 1, page_2_jid = 27)
      assert_page page_1_jid, page_2_jid

      click_pagination_link 'Next'
      assert_page page_2_jid, page_1_jid

      click_pagination_link 'Prev'
      assert_page page_1_jid, page_2_jid
    end

    it 'can paginate a group of tagged jobs' do
      build_paginated_objects do |jid|
        q.put(Qless::Job, {}, tags: ['foo'], jid: jid)
      end

      visit '/tag?tag=foo'

      test_pagination
    end

    it 'can paginate the failed jobs page' do
      build_paginated_objects do |jid|
        q.put(Qless::Job, {}, jid: jid)
        q.pop.fail('group', 'msg')
      end

      visit '/failed/group'

      # The failed jobs page shows the jobs in reverse order, for some reason.
      test_pagination 20, 1
    end

    it 'can paginate the completed jobs page' do
      build_paginated_objects do |jid|
        q.put(Qless::Job, {}, :jid => jid)
        q.pop.complete
      end

      visit '/completed'

      # The completed jobs page shows the jobs in reverse order
      test_pagination 20, 1
    end

    it 'can paginate jobs on a state filter tab' do
      q.put(Qless::Job, {}, jid: 'parent-job')

      build_paginated_objects do |jid|
        q.put(Qless::Job, {}, jid: jid, depends: ['parent-job'])
      end

      visit "/queues/#{CGI.escape(q.name)}/depends"

      test_pagination
    end

    it 'can see the root-level summary' do
      visit '/'

      # When qless is empty, we should see the default bit
      first('h1', text: /no queues/i).should be
      first('h1', text: /no failed jobs/i).should be
      first('h1', text: /no workers/i).should be

      # Alright, let's add a job to a queue, and make sure we can see it
      q.put(Qless::Job, {})
      visit '/'
      first('.queue-row', text: /testing/).should be
      first('.queue-row', text: /0\D+1\D+0\D+0\D+0/).should be
      first('h1', text: /no queues/i).should be_nil
      first('h1', text: /queues and their job counts/i).should be

      # Let's pop the job, and make sure that we can see /that/
      job = q.pop
      visit '/'
      first('.queue-row', text: /1\D+0\D+0\D+0\D+0/).should be
      first('.worker-row', text: q.worker_name).should be
      first('.worker-row', text: /1\D+0/i).should be

      # Let's complete the job, and make sure it disappears
      job.complete
      visit '/'
      first('.queue-row', text: /0\D+0\D+0\D+0\D+0/).should be
      first('.worker-row', text: /0\D+0/i).should be

      # Let's put and pop and fail a job, and make sure we see it
      q.put(Qless::Job, {})
      job = q.pop
      job.fail('foo-failure', 'bar')
      visit '/'
      first('.failed-row', text: /foo-failure/).should be
      first('.failed-row', text: /1/i).should be

      # And let's have one scheduled, and make sure it shows up accordingly
      jid = q.put(Qless::Job, {}, delay: 60)
      visit '/'
      first('.queue-row', text: /0\D+0\D+1\D+0\D+0/).should be
      # And one that depends on that job
      q.put(Qless::Job, {}, depends: [jid])
      visit '/'
      first('.queue-row', text: /0\D+0\D+1\D+0\D+1/).should be
    end

    it 'can visit the tracked page' do
      # We should be able to see all of the appropriate tabs,
      # We should be able to see all of the jobs
      jid = q.put(Qless::Job, {})
      client.jobs[jid].track

      visit '/track'
      # Make sure it appears under 'all', and 'waiting'
      first('a', text: /all\W+1/i).should be
      first('a', text: /waiting\W+1/i).should be
      # Now let's pop off the job so that it's running
      job = q.pop
      visit '/track'
      first('a', text: /all\W+1/i).should be
      first('a', text: /waiting\W+0/i).should be
      first('a', text: /running\W+1/i).should be
      # Now let's complete the job and make sure it shows up again
      job.complete
      visit '/track'
      first('a', text: /all\W+1/i).should be
      first('a', text: /running\W+0/i).should be
      first('a', text: /completed\W+1/i).should be
      job.untrack

      # And now for a scheduled job
      job = client.jobs[q.put(Qless::Job, {}, delay: 600)]
      job.track
      visit '/track'
      first('a', text: /all\W+1/i).should be
      first('a', text: /scheduled\W+1/i).should be
      job.untrack

      # And a failed job
      q.put(Qless::Job, {})
      job = q.pop
      job.track
      job.fail('foo', 'bar')
      visit '/track'
      first('a', text: /all\W+1/i).should be
      first('a', text: /failed\W+1/i).should be
      job.untrack

      # And a depends job
      job = client.jobs[
        q.put(Qless::Job, {}, depends: [q.put(Qless::Job, {})])]
      job.track
      visit '/track'
      first('a', text: /all\W+1/i).should be
      first('a', text: /depends\W+1/i).should be
      job.untrack
    end

    it 'can display the correct buttons for jobs' do
      # Depending on the state of the job, it can display
      # the appropriate buttons. In particular...
      #    - A job always gets the 'move' dropdown
      #    - A job always gets the 'track' flag
      #    - A failed job gets the 'retry' button
      #    - An incomplete job gets the 'cancel' button
      job = client.jobs[q.put(Qless::Job, {})]
      visit "/jobs/#{job.jid}"
      first('i.icon-remove').should be
      first('i.icon-repeat').should be_nil
      first('i.icon-flag').should be
      first('i.caret').should be

      # Let's fail the job and that it has the repeat button
      q.pop.fail('foo', 'bar')
      visit "/jobs/#{job.jid}"
      first('i.icon-remove').should be
      first('i.icon-repeat').should be
      first('i.icon-flag').should be
      first('i.caret').should be

      # Now let's complete the job and see that it doesn't have
      # the cancel button
      job.move('testing')
      q.pop.complete
      visit "/jobs/#{job.jid}"
      first('i.icon-remove').should be_nil
      first('i.icon-repeat').should be_nil
      first('i.icon-flag').should be
      first('i.caret').should be
    end

    it 'can display tags and priorities for jobs' do
      visit "/jobs/#{q.put(Qless::Job, {})}"
      first('input[placeholder="Pri 0"]').should be

      visit "/jobs/#{q.put(Qless::Job, {}, priority: 123)}"
      first('input[placeholder="Pri 123"]').should be

      visit "/jobs/#{q.put(Qless::Job, {}, priority: -123)}"
      first('input[placeholder="Pri -123"]').should be

      visit "/jobs/#{q.put(Qless::Job, {}, tags: %w{foo bar widget})}"
      %w{foo bar widget}.each do |tag|
        first('span', text: tag).should be
      end
    end

    it 'can track a job', js: true do
      # Make sure the job doesn't appear as tracked first, then
      # click the 'track' button, and come back to verify that
      # it's now being tracked
      jid = q.put(Qless::Job, {})
      visit '/track'
      first('a', text: /#{jid[0..5]}/i).should be_nil
      visit "/jobs/#{jid}"
      first('i.icon-flag').click
      visit '/track'
      first('a', text: /#{jid[0..5]}/i).should be
    end

    it 'can move a job', js: true do
      # Let's put a job, pop it, complete it, and then move it
      # back into the testing queue
      jid = q.put(Qless::Job, {})
      q.pop.complete
      visit "/jobs/#{jid}"
      # Get the job, check that it's complete
      client.jobs[jid].state.should eq('complete')
      first('i.caret').click
      first('a', text: 'testing').click

      # Reload the page to synchronize and ensure the AJAX request completes.
      visit "/jobs/#{jid}"

      # Now get the job again, check it's waiting
      client.jobs[jid].state.should eq('waiting')
      client.jobs[jid].queue_name.should eq('testing')
    end

    it 'can retry a single job', js: true do
      # Put, pop, and fail a job, and then click the retry button
      jid = q.put(Qless::Job, {})
      q.pop.fail('foo', 'bar')
      visit "/jobs/#{jid}"
      # Get the job, check that it's failed
      client.jobs[jid].state.should eq('failed')
      first('i.icon-repeat').click

      # Reload the page to synchronize and ensure the AJAX request completes.
      visit "/jobs/#{jid}"

      # Now get hte jobs again, check that it's waiting
      client.jobs[jid].state.should eq('waiting')
      client.jobs[jid].queue_name.should eq('testing')
    end

    it 'can cancel a single job', js: true do
      # Put, pop, and fail a job, and then click the retry button
      jid = q.put(Qless::Job, {})
      visit "/jobs/#{jid}"
      # Get the job, check that it's failed
      client.jobs[jid].state.should eq('waiting')
      first('button.btn-danger').click
      # We should have to click the cancel button now
      client.jobs[jid].should be
      first('button.btn-danger').click

      # Reload the page to synchronize and ensure the AJAX request completes.
      visit "/jobs/#{jid}"

      # /Now/ the job should be canceled
      client.jobs[jid].should be_nil
    end

    it 'can visit the configuration' do
      # Visit the bare-bones config page, make sure defaults are
      # present, then set some configurations, and then make sure
      # they appear as well
      visit '/config'
      first('h2', text: /jobs-history-count/i).should be
      first('h2', text: /stats-history/i).should be
      first('h2', text: /jobs-history/i).should be

      client.config['foo-bar'] = 50
      visit '/config'
      first('h2', text: /jobs-history-count/i).should be
      first('h2', text: /stats-history/i).should be
      first('h2', text: /jobs-history/i).should be
      first('h2', text: /foo-bar/i).should be
    end

    it 'can search by tag' do
      # We should tag some jobs, and then search by tags and ensure
      # that we find all the jobs we'd expect
      foo    = 5.times.map { |i| q.put(Qless::Job, {}, tags: ['foo']) }
      bar    = 5.times.map { |i| q.put(Qless::Job, {}, tags: ['bar']) }
      foobar = 5.times.map { |i| q.put(Qless::Job, {}, tags: %w{foo bar}) }

      visit '/tag?tag=foo'
      (foo + foobar).each do |jid|
        first('a', text: /#{jid[0..5]}/i).should be
      end

      visit '/tag?tag=bar'
      (bar + foobar).each do |jid|
        first('a', text: /#{jid[0..5]}/i).should be
      end
    end

    it 'can visit the page for a specific job' do
      # We should make sure we see details like its state, the queue
      # that it's in, its data, and any failure information
      jid = q.put(Qless::Job, { foo: 'bar' })
      job = client.jobs[jid]
      visit "/jobs/#{jid}"
      # Make sure we see its klass_name, queue, state and data
      first('h2', text: /#{job.klass}/i).should be
      first('h2', text: /#{job.queue_name}/i).should be
      first('h2', text: /#{job.state}/i).should be
      first('pre', text: /\"foo\"\s*:\s*\"bar\"/im).should be

      # Now let's pop the job and fail it just to make sure we see the error
      q.pop.fail('something-something', 'what-what')
      visit "/jobs/#{jid}"
      first('pre', text: /what-what/im).should be
    end

    it 'can visit the failed page' do
      # We should make sure that we see all the groups of failures that
      # we expect, as well as all the jobs we'd expect. This includes the
      # tabs, but also the section headings
      visit '/failed'
      first('li', text: /foo/i).should be_nil
      first('li', text: /bar/i).should be_nil

      foo = 5.times.map { |i| q.put(Qless::Job, {}) }
      q.pop(5).each { |job| job.fail('foo', 'foo-message') }
      bar = 5.times.map { |i| q.put(Qless::Job, {}) }
      q.pop(5).each { |job| job.fail('bar', 'bar-message') }
      visit '/failed'
      first('li', text: /foo\D+5/i).should be
      first('li', text: /bar\D+5/i).should be
      first('h2', text: /foo\D+5/i).should be
      first('h2', text: /bar\D+5/i).should be
      (foo + bar).each do |jid|
        first('a', text: /#{jid[0..5]}/i).should be
      end
    end

    it 'can visit the completed page' do
      foo = q.put(Qless::Job, {})
      bar = q.put(Qless::Job, {})

      visit '/completed'
      first('a', :text => /#{foo[0..5]}/i).should be_nil
      first('a', :text => /#{bar[0..5]}/i).should be_nil

      q.pop.complete
      q.pop.fail('foo', 'foo-message')

      visit '/completed'
      first('a', :text => /#{foo[0..5]}/i).should be
      first('a', :text => /#{bar[0..5]}/i).should be_nil
    end

    it 'can retry a group of failed jobs', js: true do
      # We'll fail a bunch of jobs, with two kinds of errors,
      # and then we'll make sure that we can retry all of
      # one kind, but the rest still remain failed.
      foo = 5.times.map { |i| q.put(Qless::Job, {}) }
      q.pop(5).each { |job| job.fail('foo', 'foo-message') }
      bar = 5.times.map { |i| q.put(Qless::Job, {}) }
      q.pop(5).each { |job| job.fail('bar', 'bar-message') }

      visit '/failed'
      first('li', text: /foo\D+5/i).should be
      first('h2', text: /foo\D+5/i).should be
      first('li', text: /bar\D+5/i).should be
      first('h2', text: /bar\D+5/i).should be
      (foo + bar).each do |jid|
        first('a', text: /#{jid[0..5]}/i).should be
      end

      retry_button = (all('button').select do |b|
        !b['onclick'].nil? &&
         b['onclick'].include?('retryall') &&
         b['onclick'].include?('foo')
      end).first
      retry_button.should be
      retry_button.click

      # Now we shouldn't see any of those jobs, but we should
      # still see bar jobs
      visit '/failed'
      first('li', text: /foo\D+5/i).should be_nil
      first('h2', text: /foo\D+5/i).should be_nil
      first('li', text: /bar\D+5/i).should be
      first('h2', text: /bar\D+5/i).should be
      bar.each do |jid|
        first('a', text: /#{jid[0..5]}/i).should be
      end
      foo.each do |jid|
        first('a', text: /#{jid[0..5]}/i).should be_nil
        client.jobs[jid].state.should eq('waiting')
      end
    end

    it 'can cancel a group of failed jobs', js: true do
      # We'll fail a bunch of jobs, with two kinds of errors,
      # and then we'll make sure that we can retry all of
      # one kind, but the rest still remain failed.
      foo = 5.times.map { |i| q.put(Qless::Job, {}) }
      q.pop(5).each { |job| job.fail('foo', 'foo-message') }
      bar = 5.times.map { |i| q.put(Qless::Job, {}) }
      q.pop(5).each { |job| job.fail('bar', 'bar-message') }

      visit '/failed'
      first('li', text: /foo\D+5/i).should be
      first('h2', text: /foo\D+5/i).should be
      first('li', text: /bar\D+5/i).should be
      first('h2', text: /bar\D+5/i).should be
      (foo + bar).each do |jid|
        first('a', text: /#{jid[0..5]}/i).should be
      end

      retry_button = (all('button').select do |b|
        !b['onclick'].nil? &&
         b['onclick'].include?('cancelall') &&
         b['onclick'].include?('foo')
      end).first
      retry_button.should be
      retry_button.click
      # One click ain't gonna cut it
      foo.each do |jid|
        client.jobs[jid].state.should eq('failed')
      end
      retry_button.click

      # Now we shouldn't see any of those jobs, but we should
      # still see bar jobs
      visit '/failed'
      first('li', text: /foo\D+5/i).should be_nil
      first('h2', text: /foo\D+5/i).should be_nil
      first('li', text: /bar\D+5/i).should be
      first('h2', text: /bar\D+5/i).should be
      bar.each do |jid|
        first('a', text: /#{jid[0..5]}/i).should be
      end
      foo.each do |jid|
        first('a', text: /#{jid[0..5]}/i).should be_nil
        client.jobs[jid].should be_nil
      end
    end

    it 'can change a job\'s priority', js: true do
      jid = q.put(Qless::Job, {})
      visit "/jobs/#{jid}"
      first('input[placeholder="Pri 25"]').should_not be
      first('input[placeholder="Pri 0"]').should be
      first('input[placeholder="Pri 0"]').set(25)
      first('input[placeholder="Pri 0"]').trigger('blur')

      # Now, we should make sure that the placeholder's updated,
      find('input[placeholder="Pri 25"]').should be

      # And reload the page to make sure it's stuck between reloads
      visit "/jobs/#{jid}"
      first('input[placeholder="Pri 25"]', placeholder: /\D*25/).should be
      first('input[placeholder="Pri 0"]', placeholder: /\D*0/).should_not be
    end

    it 'can add tags to a job', js: true do
      jid = q.put(Qless::Job, {})
      visit "/jobs/#{jid}"
      first('span', text: 'foo').should_not be
      first('span', text: 'bar').should_not be
      first('span', text: 'whiz').should_not be
      first('input[placeholder="Add Tag"]').set('foo')
      first('input[placeholder="Add Tag"]').trigger('blur')

      visit "/jobs/#{jid}"
      first('span', text: 'foo').should be
      first('span', text: 'bar').should_not be
      first('span', text: 'whiz').should_not be
      first('input[placeholder="Add Tag"]').set('bar')
      first('input[placeholder="Add Tag"]').trigger('blur')

      first('span', text: 'foo').should be
      first('span', text: 'bar').should be
      first('span', text: 'whiz').should_not be
      first('input[placeholder="Add Tag"]').set('foo')
      first('input[placeholder="Add Tag"]').trigger('blur')

      # Now revisit the page and make sure it's happy
      visit("/jobs/#{jid}")
      first('span', text: 'foo').should be
      first('span', text: 'bar').should be
      first('span', text: 'whiz').should_not be
    end

    it 'can remove tags', js: true do
      jid = q.put(Qless::Job, {}, tags: %w{foo bar})
      visit "/jobs/#{jid}"
      first('span', text: 'foo').should be
      first('span', text: 'bar').should be
      first('span', text: 'whiz').should_not be

      # This appears to be selenium-only, but :contains works for what we need
      first('span:contains("foo") + button').click
      # Wait for it to disappear
      first('span', text: 'foo').should_not be

      first('span:contains("bar") + button').click
      # Wait for it to disappear
      first('span', text: 'bar').should_not be
    end

    it 'can remove tags it has just added', js: true do
      jid = q.put(Qless::Job, {})
      visit "/jobs/#{jid}"
      first('span', text: 'foo').should_not be
      first('span', text: 'bar').should_not be
      first('span', text: 'whiz').should_not be
      first('input[placeholder="Add Tag"]').set('foo')
      first('input[placeholder="Add Tag"]').trigger('blur')
      first('input[placeholder="Add Tag"]').set('bar')
      first('input[placeholder="Add Tag"]').trigger('blur')
      first('input[placeholder="Add Tag"]').set('whiz')
      first('input[placeholder="Add Tag"]').trigger('blur')

      # This appears to be selenium-only, but :contains works for what we need
      first('span:contains("foo") + button').should be
      first('span:contains("foo") + button').click
      # Wait for it to disappear
      first('span', text: 'foo').should_not be

      first('span:contains("bar") + button').should be
      first('span:contains("bar") + button').click
      # Wait for it to disappear
      first('span', text: 'bar').should_not be

      first('span:contains("whiz") + button').should be
      first('span:contains("whiz") + button').click
      # Wait for it to disappear
      first('span', text: 'whiz').should_not be
    end

    it 'can sort failed groupings by the number of affected jobs' do
      # Alright, let's make 10 different failure types, and then give them
      # a certain number of jobs each, and then make sure that they stay sorted
      %w{a b c d e f g h i j}.each_with_index do |group, index|
        (index + 5).times do |i|
          q.put(Qless::Job, {})
          q.pop.fail(group, 'testing')
        end
      end

      visit '/'
      groups = all(:xpath, '//a[contains(@href, "/failed/")]')
      groups.map { |g| g.text }.join(' ').should eq('j i h g f e d c b a')

      10.times do |i|
        q.put(Qless::Job, {})
        q.pop.fail('e', 'testing')
      end

      visit '/'
      groups = all(:xpath, '//a[contains(@href, "/failed/")]')
      groups.map { |g| g.text }.join(' ').should eq('e j i h g f d c b a')
    end

    it 'can visit the various /queues/* endpoints' do
      # We should be able to see all of the appropriate tabs,
      # We should be able to see all of the jobs
      jid = q.put(Qless::Job, {})

      # We should see this job
      visit '/queues/testing/waiting'
      first('h2', text: /#{jid[0...8]}/).should be
      # Now let's pop off the job so that it's running
      job = q.pop
      visit '/queues/testing/running'
      first('h2', text: /#{jid[0...8]}/).should be
      job.complete

      # And now for a scheduled job
      job = client.jobs[q.put(Qless::Job, {}, delay: 600)]
      visit '/queues/testing/scheduled'
      first('h2', text: /#{job.jid[0...8]}/).should be
      job.cancel

      # And now a dependent job
      job = client.jobs[
        q.put(Qless::Job, {}, depends: [q.put(Qless::Job, {})])]
      visit '/queues/testing/depends'
      first('h2', text: /#{job.jid[0...8]}/).should be
      job.cancel

      # And now a recurring job
      job = client.jobs[q.recur(Qless::Job, {}, 5)]
      visit '/queues/testing/recurring'
      first('h2', text: /#{job.jid[0...8]}/).should be
      job.cancel
    end

    it 'shows the state of tracked jobs in the overview' do
      # We should be able to see all of the appropriate tabs,
      # We should be able to see all of the jobs
      jid = q.put(Qless::Job, {})
      client.jobs[jid].track

      visit '/'
      # We should see it under 'waiting'
      first('.tracked-row', text: /waiting/i).should be
      # Now let's pop off the job so that it's running
      job = q.pop
      visit '/'
      first('.tracked-row', text: /waiting/i).should_not be
      first('.tracked-row', text: /running/i).should be
      # Now let's complete the job and make sure it shows up again
      job.complete
      visit '/'
      first('.tracked-row', text: /running/i).should_not be
      first('.tracked-row', text: /complete/i).should be
      job.untrack
      first('.tracked-row', text: /complete/i).should be

      # And now for a scheduled job
      job = client.jobs[q.put(Qless::Job, {}, delay: 600)]
      job.track
      visit '/'
      first('.tracked-row', text: /scheduled/i).should be
      job.untrack

      # And a failed job
      q.put(Qless::Job, {})
      job = q.pop
      job.track
      job.fail('foo', 'bar')
      visit '/'
      first('.tracked-row', text: /failed/i).should be
      job.untrack

      # And a depends job
      job = client.jobs[
        q.put(Qless::Job, {}, depends: [q.put(Qless::Job, {})])]
      job.track
      visit '/'
      first('.tracked-row', text: /depends/i).should be
      job.untrack
    end

    it 'can display, cancel, move recurring jobs', js: true do
      # We should create a recurring job and then make sure we can see it
      jid = q.recur(Qless::Job, {}, 600)

      visit "/jobs/#{jid}"
      first('h2', text: jid[0...8]).should be
      first('h2', text: 'recurring').should be
      first('h2', text: 'testing').should be
      first('h2', text: 'Qless::Job').should be
      first('button.btn-danger').should be
      first('i.caret').should be

      # Cancel it
      first('button.btn-danger').click
      # We should have to click the cancel button now
      first('button.btn-danger').click
      client.jobs[jid].should_not be

      # Move it to another queue
      jid = other.recur(Qless::Job, {}, 600)
      client.jobs[jid].queue_name.should eq('other')
      visit "/jobs/#{jid}"
      first('i.caret').click
      first('a', text: 'testing').click
      # Now get the job again, check it's waiting
      client.jobs[jid].queue_name.should eq('testing')
    end

    it 'can change recurring job priorities', js: true do
      jid = q.recur(Qless::Job, {}, 600)
      visit "/jobs/#{jid}"
      first('input[placeholder="Pri 25"]').should_not be
      first('input[placeholder="Pri 0"]').should be
      first('input[placeholder="Pri 0"]').set(25)
      first('input[placeholder="Pri 0"]').trigger('blur')

      # Now, we should make sure that the placeholder's updated,
      find('input[placeholder="Pri 25"]').should be

      # And reload the page to make sure it's stuck between reloads
      visit "/jobs/#{jid}"
      first('input[placeholder="Pri 25"]', placeholder: /\D*25/).should be
      first('input[placeholder="Pri 0"]', placeholder: /\D*0/).should_not be
    end

    it 'can pause and unpause a queue', js: true do
      10.times do
        q.put(Qless::Job, {})
      end
      visit '/'

      q.pop.should be
      button = first('button', title: /Pause/)
      button.should be
      button.click
      q.pop.should_not be

      # Now we should unpause it
      first('button', title: /Unpause/).click
      q.pop.should be
    end

    it 'can add tags to a recurring job', js: true do
      jid = q.put(Qless::Job, {})
      visit "/jobs/#{jid}"
      first('span', text: 'foo').should_not be
      first('span', text: 'bar').should_not be
      first('span', text: 'whiz').should_not be
      first('input[placeholder="Add Tag"]').set('foo')
      first('input[placeholder="Add Tag"]').trigger('blur')

      find('span', text: 'foo').should be
      first('span', text: 'bar').should_not be
      first('span', text: 'whiz').should_not be
      first('input[placeholder="Add Tag"]').set('bar')
      first('input[placeholder="Add Tag"]').trigger('blur')

      find('span', text: 'foo').should be
      find('span', text: 'bar').should be
      first('span', text: 'whiz').should_not be
      first('input[placeholder="Add Tag"]').set('foo')
      first('input[placeholder="Add Tag"]').trigger('blur')

      # Now revisit the page and make sure it's happy
      visit("/jobs/#{jid}")
      find('span', text: 'foo').should be
      find('span', text: 'bar').should be
      first('span', text: 'whiz').should_not be
    end
  end

  describe 'Rack Tests', :integration do
    include Rack::Test::Methods

    # Our main test queue
    let(:q)      { client.queues['testing'] }
    let(:app)    { Qless::Server.new(Qless::Client.new(redis_config)) }

    it 'can access the JSON endpoints for queue sizes' do
      q.put(Qless::Job, {})
      get '/queues.json'
      response = {
          'running'   => 0,
          'name'      => 'testing',
          'waiting'   => 1,
          'recurring' => 0,
          'depends'   => 0,
          'stalled'   => 0,
          'scheduled' => 0,
          'paused'    => false
        }
      JSON.parse(last_response.body).should eq([response])

      get '/queues/testing.json'
      JSON.parse(last_response.body).should eq(response)
    end

    it 'can access the JSON endpoint for failures' do
      get '/failed.json'
      JSON.parse(last_response.body).should eq({})

      # Now, put a job in, pop it and fail it, make sure we see
      q.put(Qless::Job, {})
      job = q.pop
      job.fail('foo', 'bar')
      get '/failed.json'
      JSON.parse(last_response.body).should eq({ 'foo' => 1 })
    end
  end
end
