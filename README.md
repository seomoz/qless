qless
=====

Qless is a `Redis`-based job queueing system inspired by `resque`, but built on
a collection of Lua scripts, maintained in the [qless-core](https://github.com/seomoz/qless-core)
repo.

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
pay it any mind every again. A job can be `dropped`, which is when a worker fails
to heartbeat or complete the job in a timely fashion, or a job can be `failed`,
which is when a host recognizes some systematically problematic state about the
job. A worker should only fail a job if the error is likely not a transient one;
otherwise, that worker should just drop it and let the system reclaim it.

Features
========

1. __Client Managed__ -- Just run a Redis 2.6 instance, point your workers at it.
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
  inserted. Otherwise, it's like `nice`ness; lower number means popped sooner\
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

Using
=====
First things first, require `qless` and create a client. The client accepts all the
same arguments that you'd use when constructing a redis client.

  require 'qless'

  # Connect to localhost
  client = Qless::Client.new
  # Connect to somewhere else
  client = Qless::Client.new(:host => 'foo.bar.com', :port => 1234)

Now you can access a queue, and add a job to that queue.

  # This references a new or existing queue 'testing'
  queue = client.queues['testing']
  # Let's add a job, with some data. Returns Job ID
  queue.put(MyJobClass, :hello => 'howdy')
  # => "0c53b0404c56012f69fa482a1427ab7d"
  # Now we can ask for a job
  job = queue.pop
  # => < Qless::Job 0c53b0404c56012f69fa482a1427ab7d >
  # And we can do the work associated with it!
  job.process

Running A Worker
================

Web Interface
=============

Job Dependencies
================
Let's say you have one job that depends on another, but the task definitions are
fundamentally different. You need to bake a turkey, and you need to make stuffing,
but you can't make the turkey until the stuffing is made:

  queue    = client.queues['cook']
  stuffing = queue.put(MakeStuffing, {:lots => 'of butter'})
  turkey   = queue.put(MakeTurkey  , {:with => 'stuffing'}, :depends=>[stuffing])

When the stuffing job completes, the turkey job is unlocked and free to be processed.

Priority
========
Some jobs need to get popped sooner than others. Whether it's a trouble ticket, or
debugging, you can do this pretty easily when you put a job in a queue:

  queue.put(MyJobClass, {:foo => 'bar'}, :priority => -10)

What happens when you want to adjust a job's priority while it's still waiting in
a queue?

  job = client.jobs['0c53b0404c56012f69fa482a1427ab7d']
  job.priority = -10
  # Now this will get popped before any job of lesser priority

Scheduled Jobs
==============
If you don't want a job to be run right away but some time in the future, you can
specify a delay:

  # Run at least 10 minutes from now
  queue.put(MyJobClass, {:foo => 'bar'}, :delay => 600)

This doesn't guarantee that job will be run exactly at 10 minutes. You can accomplish
this by changing the job's priority so that once 10 minutes has elapsed, it's put before
lesser-priority jobs:

  # Run in 10 minutes
  queue.put(MyJobClass, {:foo => 'bar'}, :delay => 600, :priority => -100)

Recurring Jobs
==============
Sometimes it's not enough simply to schedule one job, but you want to run jobs regularly.
In particular, maybe you have some batch operation that needs to get run once an hour and
you don't care what worker runs it. Recurring jobs are specified much like other jobs:

  # Run every hour
  queue.recur(MyJobClass, {:widget => 'warble'}, 3600)
  # => 22ac75008a8011e182b24cf9ab3a8f3b

You can even access them in much the same way as you would normal jobs:

  job = client.jobs['22ac75008a8011e182b24cf9ab3a8f3b']
  # => < Qless::RecurringJob 22ac75008a8011e182b24cf9ab3a8f3b >

Changing the interval at which it runs after the fact is trivial:

  # I think I only need it to run once every two hours
  job.interval = 7200

If you want it to run every hour on the hour, but it's 2:37 right now, you can specify
an offset which is how long it should wait before popping the first job:

  # 23 minutes of waiting until it should go
  queue.recur(MyJobClass, {:howdy => 'hello'}, 3600, :offset => 23 * 60)

Recurring jobs also have priority, a configurable number of retries, and tags. These
settings don't apply to the recurring jobs, but rather the jobs that they create. In the
case where more than one interval passes before a worker tries to pop the job, __more than
one job is created__. The thinking is that while it's completely client-managed, the state
should not be dependent on how often workers are trying to pop jobs.

  # Recur every minute
  queue.recur(MyJobClass, {:lots => 'of jobs'}, 60)
  # Wait 5 minutes
  queue.pop(10).length
  # => 5 jobs got popped

Configuration Options
=====================
You can get and set global (read: in the context of the same Redis instance) configuration
to change the behavior for heartbeating, and so forth. There aren't a tremendous number
of configuration options, but an important one is how long job data is kept around. Job
data is expired after it has been completed for `jobs-history` seconds, but is limited to
the last `jobs-history-count` completed jobs. These default to 50k jobs, and 30 days, but
depending on volume, your needs may change. To only keep the last 500 jobs for up to 7 days:

  client.config['jobs-history'] = 7 * 86400
  client.config['jobs-history-count'] = 500

Tagging / Tracking
==================
In qless, 'tracking' means flagging a job as important. Tracked jobs have a tab reserved
for them in the web interface, and they also emit subscribable events as they make progress
(more on that below). You can flag a job from the web interface, or the corresponding code:

  client.jobs['b1882e009a3d11e192d0b174d751779d'].track

Jobs can be tagged with strings which are indexed for quick searches. For example, jobs
might be associated with customer accounts, or some other key that makes sens for your
project.

  queue.put(MyJobClass, {:tags => 'aplenty'}, :tags => ['12345', 'foo', 'bar'])

This makes them searchable in the web interface, or from code:

  jids = client.jobs.tagged('foo')

You can add or remove tags at will, too:

  job = client.jobs['b1882e009a3d11e192d0b174d751779d']
  job.tag('howdy', 'hello')
  job.untag('foo', 'bar')

Notifications
=============
Tracked jobs emit events on specific pubsub channels as things happen to them. Whether
it's getting popped off of a queue, completed by a worker, etc. A good example of how
to make use of this is in the `qless-campfire` or `qless-growl`. The jist of it goes like
this, though:

  client.events do |on|
    on.canceled  { |jid| puts "#{jid} canceled"   }
    on.stalled   { |jid| puts "#{jid} stalled"    }
    on.track     { |jid| puts "tracking #{jid}"   }
    on.untrack   { |jid| puts "untracking #{jid}" }
    on.completed { |jid| puts "#{jid} completed"  }
    on.failed    { |jid| puts "#{jid} failed"     }
    on.popped    { |jid| puts "#{jid} popped"     }
    on.put       { |jid| puts "#{jid} put"        }
  end

Those familiar with redis pubsub will note that a redis connection can only be used
for pubsub-y commands once listening. For this reason, invoking `client.events` actually
creates a second connection so that `client` can still be used as it normally would be:

  client.events do |on|
    on.failed do |jid|
    puts "#{jid} failed in #{client.jobs[jid].queue_name}"
    end
  end

Heartbeating
============
When a worker is given a job, it is given an exclusive lock to that job. That means
that job won't be given to any other worker, so long as the worker checks in with
progress on the job. By default, jobs have to either report back progress every 60
seconds, or complete it, but that's a configurable option. For longer jobs, this
may not make sense.

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

If you want to increase the heartbeat in all queues,

  # Now jobs get 10 minutes to check in
  client.config['heartbeat'] = 600
  # But the testing queue doesn't get as long.
  client.queues['testing'].heartbeat = 300

When choosing a heartbeat interval, realize that this is the amount of time that
can pass before qless realizes if a job has been dropped. At the same time, you don't
want to burden qless with heartbeating every 10 seconds if your job is expected to
take several hours.

An idiom you're encouraged to use for long-running jobs that want to check in their
progress periodically:

  # Wait until we have 5 minutes left on the heartbeat, and if we find that
  # we've lost our lock on a job, then honorable fall on our sword
  if (job.ttl < 300) && !job.heartbeat
    return / die / exit
  end

Stats
=====
One nice feature of `qless` is that you can get statistics about usage. Stats are
binned by day, so when you want stats about a queue, you need to say what queue
and what day you're talking about. By default, you just get the stats for today.
These stats include information about the mean job wait time, standard deviation,
and histogram. This same data is also provided for job completion:

  # So, how're we doing today?
  stats = client.stats.get('testing')
  # => { 'run' => {'mean' => ..., }, 'wait' => {'mean' => ..., }}

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
