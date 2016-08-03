qless [![Build Status](https://travis-ci.org/seomoz/qless.svg?branch=master)](https://travis-ci.org/seomoz/qless)
=====

Qless is a powerful `Redis`-based job queueing system inspired by
[resque](https://github.com/defunkt/resque#readme),
but built on a collection of Lua scripts, maintained in the
[qless-core](https://github.com/seomoz/qless-core) repo.

Philosophy and Nomenclature
===========================
A `job` is a unit of work identified by a job id or `jid`. A `queue` can contain
several jobs that are scheduled to be run at a certain time, several jobs that are
waiting to run, and jobs that are currently running. A `worker` is a process on a
host, identified uniquely, that asks for jobs from the queue, performs some process
associated with that job, and then marks it as complete. When it's completed, it
can be put into another queue.

Jobs can only be in one queue at a time. That queue is whatever queue they were last
put in. So if a worker is working on a job, and you move it, the worker's request to
complete the job will be ignored.

A job can be `canceled`, which means it disappears into the ether, and we'll never
pay it any mind ever again. A job can be `dropped`, which is when a worker fails
to heartbeat or complete the job in a timely fashion, or a job can be `failed`,
which is when a host recognizes some systematically problematic state about the
job. A worker should only fail a job if the error is likely not a transient one;
otherwise, that worker should just drop it and let the system reclaim it.

Features
========

1. __Jobs don't get dropped on the floor__ -- Sometimes workers drop jobs. Qless
  automatically picks them back up and gives them to another worker
1. __Tagging / Tracking__ -- Some jobs are more interesting than others. Track those
  jobs to get updates on their progress. Tag jobs with meaningful identifiers to
  find them quickly in the UI.
1. __Job Dependencies__ -- One job might need to wait for another job to complete
1. __Stats__ -- `qless` automatically keeps statistics about how long jobs wait
  to be processed and how long they take to be processed. Currently, we keep
  track of the count, mean, standard deviation, and a histogram of these times.
1. __Job data is stored temporarily__ -- Job info sticks around for a configurable
  amount of time so you can still look back on a job's history, data, etc.
1. __Priority__ -- Jobs with the same priority get popped in the order they were
  inserted; a higher priority means that it gets popped faster
1. __Retry logic__ -- Every job has a number of retries associated with it, which are
  renewed when it is put into a new queue or completed. If a job is repeatedly
  dropped, then it is presumed to be problematic, and is automatically failed.
1. __Web App__ -- With the advent of a Ruby client, there is a Sinatra-based web
  app that gives you control over certain operational issues
1. __Scheduled Work__ -- Until a job waits for a specified delay (defaults to 0),
  jobs cannot be popped by workers
1. __Recurring Jobs__ -- Scheduling's all well and good, but we also support
  jobs that need to recur periodically.
1. __Notifications__ -- Tracked jobs emit events on pubsub channels as they get
  completed, failed, put, popped, etc. Use these events to get notified of
  progress on jobs you're interested in.

Enqueing Jobs
=============
First things first, require `qless` and create a client. The client accepts all the
same arguments that you'd use when constructing a redis client.

``` ruby
require 'qless'

# Connect to localhost
client = Qless::Client.new
# Connect to somewhere else
client = Qless::Client.new(:host => 'foo.bar.com', :port => 1234)
```

Jobs should be classes or modules that define a `perform` method, which
must accept a single `job` argument:

``` ruby
class MyJobClass
  def self.perform(job)
    # job is an instance of `Qless::Job` and provides access to
    # job.data, a means to cancel the job (job.cancel), and more.
  end
end
```

Now you can access a queue, and add a job to that queue.

``` ruby
# This references a new or existing queue 'testing'
queue = client.queues['testing']
# Let's add a job, with some data. Returns Job ID
queue.put(MyJobClass, :hello => 'howdy')
# => "0c53b0404c56012f69fa482a1427ab7d"
# Now we can ask for a job
job = queue.pop
# => <Qless::Job 0c53b0404c56012f69fa482a1427ab7d (MyJobClass / testing)>
# And we can do the work associated with it!
job.perform
```

The job data must be serializable to JSON, and it is recommended
that you use a hash for it. See below for a list of the supported job options.

The argument returned by `queue.put` is the job ID, or jid. Every Qless
job has a unique jid, and it provides a means to interact with an
existing job:

``` ruby
# find an existing job by it's jid
job = client.jobs[jid]

# Query it to find out details about it:
job.klass # => the class of the job
job.queue # => the queue the job is in
job.data  # => the data for the job
job.history # => the history of what has happened to the job sofar
job.dependencies # => the jids of other jobs that must complete before this one
job.dependents # => the jids of other jobs that depend on this one
job.priority # => the priority of this job
job.tags # => array of tags for this job
job.original_retries # => the number of times the job is allowed to be retried
job.retries_left # => the number of retries left

# You can also change the job in various ways:
job.requeue("some_other_queue") # move it to a new queue
job.cancel # cancel the job
job.tag("foo") # add a tag
job.untag("foo") # remove a tag
```

Running A Worker
================

The Qless ruby worker was heavily inspired by Resque's worker,
but thanks to the power of the qless-core lua scripts, it is
*much* simpler and you are welcome to write your own (e.g. if
you'd rather save memory by not forking the worker for each job).

As with resque...

* The worker forks a child process for each job in order to provide
   resilience against memory leaks. Pass the `RUN_AS_SINGLE_PROCESS`
   environment variable to force Qless to not fork the child process.
   Single process mode should only be used in some test/dev
   environments.
* The worker updates its procline with its status so you can see
  what workers are doing using `ps`.
* The worker registers signal handlers so that you can control it
  by sending it signals.
* The worker is given a list of queues to pop jobs off of.
* The worker logs out put based on `VERBOSE` or `VVERBOSE` (very
  verbose) environment variables.
* Qless ships with a rake task (`qless:work`) for running workers.
  It runs `qless:setup` before starting the main work loop so that
  users can load their environment in that task.
* The sleep interval (for when there is no jobs available) can be
  configured with the `INTERVAL` environment variable.

Resque uses queues for its notion of priority. In contrast, qless
has priority support built-in. Thus, the worker supports two strategies
for what order to pop jobs off the queues: ordered and round-robin.
The ordered reserver will keep popping jobs off the first queue until
it is empty, before trying to pop job off the second queue. The
round-robin reserver will pop a job off the first queue, then the second
queue, and so on. You could also easily implement your own.

To start a worker, write a bit of Ruby code that instantiates a
worker and runs it. You could write a rake task to do this, for
example:

``` ruby
namespace :qless do
  desc "Run a Qless worker"
  task :work do
    # Load your application code. All job classes must be loaded.
    require 'my_app/environment'

    # Require the parts of qless you need
    require 'qless'
    require 'qless/job_reservers/ordered'
    require 'qless/worker'

    # Create a client
    client = Qless::Client.new(:host => 'foo.bar.com', :port => 1234)

    # Get the queues you use
    queues = %w[ queue_1 queue_2 ].map do |name|
      client.queues[name]
    end

    # Create a job reserver; different reservers use different
    # strategies for which order jobs are popped off of queues
    reserver = Qless::JobReservers::Ordered.new(queues)

    # Create a forking worker that uses the given reserver to pop jobs.
    worker = Qless::Workers::ForkingWorker.new(reserver)

    # Start the worker!
    worker.run
  end
end
```

The following signals are supported in the parent process:

* TERM: Shutdown immediately, stop processing jobs.
*  INT: Shutdown immediately, stop processing jobs.
* QUIT: Shutdown after the current job has finished processing.
* USR1: Kill the forked child immediately, continue processing jobs.
* USR2: Don't process any new jobs, and dump the current backtrace.
* CONT: Start processing jobs again after a USR2

You should send these to the master process, not the child.

The child process supports the `USR2` signal, whch causes it to
dump its current backtrace.

Workers also support middleware modules that can be used to inject
logic before, after or around the processing of a single job in
the child process. This can be useful, for example, when you need to
re-establish a connection to your database in each job.

Define a module with an `around_perform` method that calls `super` where you
want the job to be processed:

``` ruby
module ReEstablishDBConnection
  def around_perform(job)
    MyORM.establish_connection
    super
  end
end
```

Then, mix-it into the worker class. You can mix-in as many
middleware modules as you like:

``` ruby
require 'qless/worker'
Qless::Worker.class_eval do
  include ReEstablishDBConnection
  include SomeOtherAwesomeMiddleware
end
```

Per-Job Middlewares
===================

Qless also supports middleware on a per-job basis, when you have some
orthogonal logic to run in the context of some (but not all) jobs.

Per-job middlewares are defined the same as worker middlewares:

``` ruby
module ReEstablishDBConnection
  def around_perform(job)
    MyORM.establish_connection
    super
  end
end
```

To add them to a job class, you first have to make your job class
middleware-enabled by extending it with
`Qless::Job::SupportsMiddleware`, then extend your middleware
modules:

``` ruby
class MyJobClass
  extend Qless::Job::SupportsMiddleware
  extend ReEstablishDBConnection
  extend MyOtherAwesomeMiddleware

  def self.perform(job)
  end
end
```

Note that `Qless::Job::SupportsMiddleware` must be extended onto your
job class _before_ any other middleware modules.

Web Interface
=============

Qless ships with a resque-inspired web app that lets you easily
deal with failures and see what it is processing. If you're project
has a rack-based ruby web app, we recommend you mount Qless's web app
in it. Here's how you can do that with `Rack::Builder` in your `config.ru`:

``` ruby
client = Qless::Client.new(:host => "some-host", :port => 7000)

Rack::Builder.new do
  use SomeMiddleware

  map('/some-other-app') { run Apps::Something.new }
  map('/qless')          { run Qless::Server.new(client) }
end
```

For an app using Rails 3+, check the router documentation for how to mount
rack apps.

Job Dependencies
================
Let's say you have one job that depends on another, but the task definitions are
fundamentally different. You need to bake a turkey, and you need to make stuffing,
but you can't make the turkey until the stuffing is made:

``` ruby
queue        = client.queues['cook']
stuffing_jid = queue.put(MakeStuffing, {:lots => 'of butter'})
turkey_jid   = queue.put(MakeTurkey  , {:with => 'stuffing'}, :depends=>[stuffing_jid])
```

When the stuffing job completes, the turkey job is unlocked and free to be processed.

Priority
========
Some jobs need to get popped sooner than others. Whether it's a trouble ticket, or
debugging, you can do this pretty easily when you put a job in a queue:

``` ruby
queue.put(MyJobClass, {:foo => 'bar'}, :priority => 10)
```

What happens when you want to adjust a job's priority while it's still waiting in
a queue?

``` ruby
job = client.jobs['0c53b0404c56012f69fa482a1427ab7d']
job.priority = 10
# Now this will get popped before any job of lower priority
```

Scheduled Jobs
==============
If you don't want a job to be run right away but some time in the future, you can
specify a delay:

``` ruby
# Run at least 10 minutes from now
queue.put(MyJobClass, {:foo => 'bar'}, :delay => 600)
```

This doesn't guarantee that job will be run exactly at 10 minutes. You can accomplish
this by changing the job's priority so that once 10 minutes has elapsed, it's put before
lesser-priority jobs:

``` ruby
# Run in 10 minutes
queue.put(MyJobClass, {:foo => 'bar'}, :delay => 600, :priority => 100)
```

Recurring Jobs
==============
Sometimes it's not enough simply to schedule one job, but you want to run jobs regularly.
In particular, maybe you have some batch operation that needs to get run once an hour and
you don't care what worker runs it. Recurring jobs are specified much like other jobs:

``` ruby
# Run every hour
queue.recur(MyJobClass, {:widget => 'warble'}, 3600)
# => 22ac75008a8011e182b24cf9ab3a8f3b
```

You can even access them in much the same way as you would normal jobs:

``` ruby
job = client.jobs['22ac75008a8011e182b24cf9ab3a8f3b']
# => < Qless::RecurringJob 22ac75008a8011e182b24cf9ab3a8f3b >
```

Changing the interval at which it runs after the fact is trivial:

``` ruby
# I think I only need it to run once every two hours
job.interval = 7200
```

If you want it to run every hour on the hour, but it's 2:37 right now, you can specify
an offset which is how long it should wait before popping the first job:

``` ruby
# 23 minutes of waiting until it should go
queue.recur(MyJobClass, {:howdy => 'hello'}, 3600, :offset => 23 * 60)
```

Recurring jobs also have priority, a configurable number of retries, and tags. These
settings don't apply to the recurring jobs, but rather the jobs that they create. In the
case where more than one interval passes before a worker tries to pop the job, __more than
one job is created__. The thinking is that while it's completely client-managed, the state
should not be dependent on how often workers are trying to pop jobs.

``` ruby
  # Recur every minute
  queue.recur(MyJobClass, {:lots => 'of jobs'}, 60)
  # Wait 5 minutes
  queue.pop(10).length
  # => 5 jobs got popped
```

Configuration Options
=====================
You can get and set global (read: in the context of the same Redis instance) configuration
to change the behavior for heartbeating, and so forth. There aren't a tremendous number
of configuration options, but an important one is how long job data is kept around. Job
data is expired after it has been completed for `jobs-history` seconds, but is limited to
the last `jobs-history-count` completed jobs. These default to 50k jobs, and 30 days, but
depending on volume, your needs may change. To only keep the last 500 jobs for up to 7 days:

``` ruby
client.config['jobs-history'] = 7 * 86400
client.config['jobs-history-count'] = 500
```

Tagging / Tracking
==================
In qless, 'tracking' means flagging a job as important. Tracked jobs have a tab reserved
for them in the web interface, and they also emit subscribable events as they make progress
(more on that below). You can flag a job from the web interface, or the corresponding code:

``` ruby
client.jobs['b1882e009a3d11e192d0b174d751779d'].track
```

Jobs can be tagged with strings which are indexed for quick searches. For example, jobs
might be associated with customer accounts, or some other key that makes sense for your
project.

``` ruby
queue.put(MyJobClass, {:tags => 'aplenty'}, :tags => ['12345', 'foo', 'bar'])
```

This makes them searchable in the web interface, or from code:

``` ruby
jids = client.jobs.tagged('foo')
```

You can add or remove tags at will, too:

``` ruby
job = client.jobs['b1882e009a3d11e192d0b174d751779d']
job.tag('howdy', 'hello')
job.untag('foo', 'bar')
```

Notifications
=============
Tracked jobs emit events on specific pubsub channels as things happen to them. Whether
it's getting popped off of a queue, completed by a worker, etc. A good example of how
to make use of this is in the `qless-campfire` or `qless-growl`. The jist of it goes like
this, though:

``` ruby
client.events.listen do |on|
  on.canceled  { |jid| puts "#{jid} canceled"   }
  on.stalled   { |jid| puts "#{jid} stalled"    }
  on.track     { |jid| puts "tracking #{jid}"   }
  on.untrack   { |jid| puts "untracking #{jid}" }
  on.completed { |jid| puts "#{jid} completed"  }
  on.failed    { |jid| puts "#{jid} failed"     }
  on.popped    { |jid| puts "#{jid} popped"     }
  on.put       { |jid| puts "#{jid} put"        }
end
```

Those familiar with redis pubsub will note that a redis connection can only be used
for pubsub-y commands once listening. For this reason, invoking `client.events` actually
creates a second connection so that `client` can still be used as it normally would be:

``` ruby
client.events do |on|
  on.failed do |jid|
  puts "#{jid} failed in #{client.jobs[jid].queue_name}"
  end
end
```

Heartbeating
============
When a worker is given a job, it is given an exclusive lock to that job. That means
that job won't be given to any other worker, so long as the worker checks in with
progress on the job. By default, jobs have to either report back progress every 60
seconds, or complete it, but that's a configurable option. For longer jobs, this
may not make sense.

``` ruby
# Hooray! We've got a piece of work!
job = queue.pop
# How long until I have to check in?
job.ttl
# => 59
# Hey! I'm still working on it!
job.heartbeat
# => 1331326141.0
# Ok, I've got some more time. Oh! Now I'm done!
job.complete
```

If you want to increase the heartbeat in all queues,

``` ruby
# Now jobs get 10 minutes to check in
client.config['heartbeat'] = 600
# But the testing queue doesn't get as long.
client.queues['testing'].heartbeat = 300
```

When choosing a heartbeat interval, realize that this is the amount of time that
can pass before qless realizes if a job has been dropped. At the same time, you don't
want to burden qless with heartbeating every 10 seconds if your job is expected to
take several hours.

An idiom you're encouraged to use for long-running jobs that want to check in their
progress periodically:

``` ruby
# Wait until we have 5 minutes left on the heartbeat, and if we find that
# we've lost our lock on a job, then honorably fall on our sword
if (job.ttl < 300) && !job.heartbeat
  return / die / exit
end
```

Stats
=====
One nice feature of `qless` is that you can get statistics about usage. Stats are
aggregated by day, so when you want stats about a queue, you need to say what queue
and what day you're talking about. By default, you just get the stats for today.
These stats include information about the mean job wait time, standard deviation,
and histogram. This same data is also provided for job completion:

``` ruby
# So, how're we doing today?
stats = client.stats.get('testing')
# => { 'run' => {'mean' => ..., }, 'wait' => {'mean' => ..., }}
```

Time
====
It's important to note that Redis doesn't allow access to the system time if you're
going to be making any manipulations to data (which our scripts do). And yet, we
have heartbeating. This means that the clients actually send the current time when
making most requests, and for consistency's sake, means that your workers must be
relatively synchronized. This doesn't mean down to the tens of milliseconds, but if
you're experiencing appreciable clock drift, you should investigate NTP. For what it's
worth, this hasn't been a problem for us, but most of our jobs have heartbeat intervals
of 30 minutes or more.

Ensuring Job Uniqueness
=======================

As mentioned above, Jobs are uniquely identied by an id--their jid.
Qless will generate a UUID for each enqueued job or you can specify
one manually:

``` ruby
queue.put(MyJobClass, { :hello => 'howdy' }, :jid => 'my-job-jid')
```

This can be useful when you want to ensure a job's uniqueness: simply
create a jid that is a function of the Job's class and data, it'll
guaranteed that Qless won't have multiple jobs with the same class
and data.

Setting Default Job Options
===========================

`Qless::Queue#put` accepts a number of job options (see above for their
semantics):

* jid
* delay
* priority
* tags
* retries
* depends

When enqueueing the same kind of job with the same args in multiple
places it's a pain to have to declare the job options every time.
Instead, you can define default job options directly on the job class:

``` ruby
class MyJobClass
  def self.default_job_options(data)
    { :priority => 10, :delay => 100 }
  end
end

queue.put(MyJobClass, { :some => "data" }, :delay => 10)
```

Individual jobs can still specify options, so in this example,
the job would be enqueued with a priority of 10 and a delay of 10.

Testing Jobs
============
When unit testing your jobs, you will probably want to avoid the
overhead of round-tripping them through redis. You can of course
use a mock job object and pass it to your job class's `perform`
method. Alternately, if you want a real full-fledged `Qless::Job`
instance without round-tripping it through Redis, use `Qless::Job.build`:

``` ruby
describe MyJobClass do
  let(:client) { Qless::Client.new }
  let(:job)    { Qless::Job.build(client, MyJobClass, :data => { "some" => "data" }) }

  it 'does something' do
    MyJobClass.perform(job)
    # make an assertion about what happened
  end
end
```

The options hash passed to `Qless::Job.build` supports all the same
options a normal job supports. See
[the source](https://github.com/seomoz/qless/blob/master/lib/qless/job.rb)
for a full list.

Contributing
============

To bootstrap an environment, first [have a redis](https://github.com/seomoz/qless/wiki/Bootstrapping-Qless#a-simple-redis-bootstrap).

Have `rvm` or `rbenv`.  Then to install the dependencies:

```bash
rbenv install                 # rbenv only.  Install bundler if you need it.
bundle install
./exe/install_phantomjs       # Bring in phantomjs 1.7.0 for tests.
rbenv rehash                  # rbenv only
git submodule init
git submodule update
bundle exec rake core:build
```

To run the tests:

```
bundle exec rake spec
```

**The locally installed redis will be flushed before and after each test run.**

To change the redis instance used in tests, put the connection information into [`./spec/redis.config.yml`](https://github.com/seomoz/qless/blob/92904532aee82aaf1078957ccadfa6fcd27ae408/spec/spec_helper.rb#L26).

To contribute, fork the repo, use feature branches, run the tests and open PRs.

Mailing List
============

For questions and general Qless discussion, please join the [Qless
Mailing list](https://groups.google.com/forum/?fromgroups#!forum/qless).

