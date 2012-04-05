qless
=====

Qless is a `Redis`-based job queueing system inspired by `resque`, but built on
a collection of Lua scripts, maintained in the [qless-core](https://github.com/seomoz/qless-core)
repo.

Philosophy and Nomenclature
===========================

A `job` is a unit of work. A `queue` can contain several jobs that are scheduled to
be run at a certain time, several jobs that are waiting to run, and jobs that are
currently running. A `worker` is a process on a host, identified uniquely, that asks
for jobs from the queue, performs some process associated with that job, and then
marks it as complete. When it's completed, it can be put into another queue.

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

1. __Jobs don't get dropped on the floor__ -- You can write a flakey worker,
	(although you should try to avoid it), that occasionally forgets to compete
	a job, and `qless` recognizes this, and will hand it out to another worker.
1. __Stats__ -- `qless` automatically keeps statistics about how long jobs wait
	to be processed and how long they take to be processed. Currently, we keep
	track of the count, mean, standard deviation, and a histogram of these times.
1. __Job data is stored temporarily__ -- All jobs have a unique identifier, and
	if you want to get information about a specific job, then you can get it. 
	However, job data expires in a configurable way. Only a history of the last
	__jobs-history__ time is kept for completed jobs, and only the most recent
	__jobs-history-count__ jobs.
1. __Priority__ -- Jobs have a priority, which determines the order in which it
	is selected to be given to a worker. When two jobs have equal priority, the
	job that was inserted first wins.
1. __High Performance__ -- Redis is blazingly fast, and as a consequence, so is
	`qless`. See the Benchmarks for more information, but I've seen it handle 
	on the order fo 4500 job pop/completes per second on a MacBook Pro.
1. __Scheduled Work__ -- Jobs can be scheduled to run in a queue as some specified
	time. Until that time, it is ignored, at which point, it's put in the queue with
	its original priority.
1. __Convenient Clients__ -- The lingua franca for `qless` is JavaScript, since most
	languages have good support for it. The library itself is a collection of `Lua`
	scripts that are actually run within the `Redis` server, which means performance,
	atomicity, and that clients are easy to write. Clients are merely responsible for
	providing a reasonable structure from which to invoke these Lua scripts.
1. __Retry logic__ -- Every job has a number of retries associated with it, which are
	renewed when it is put into a new queue or completed. If a job is repeatedly 
	dropped, then it is presumed to be problematic, and is automatically failed.
1. __Tagging / Tracking__ -- Mark jobs you're interested in (perhaps associated with 
	testing, or user issues) for tracking, and `qless` will maintain a list of such
	jobs for you. At this time you can also provide additional tags to help identify
	the job when displayed. I've personally found this features _extremely_ helpful
	when monitoring jobs associated with user issues.
1. __Worker Can Have More Than One Job__ -- Some jobs can be done in parallel, and
	as such, a worker can take on multiple jobs. The requirements for this worker
	are the same -- it must maintain the locks on all the jobs it has, but so long
	as it does, this is considered valid. For example, a worker might a crawler,
	in which case it problably wants to concurrently be crawling several different
	sets of urls.
1. __Worker Data__ -- We keep track of the jobs that a worker has locks on at any
	given time for quick access. The API endpoint also reports which of those jobs
	are stalled and which are active, and can list all the known workers.

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
	queue = client.queue('testing')
	# Let's add a job, with some data. Returns Job ID
	queue.put(:hello => 'howdy')
	# => "0c53b0404c56012f69fa482a1427ab7d"
	# Now we can ask for a job
	job = queue.pop
	# => < Qless::Job 0c53b0404c56012f69fa482a1427ab7d >

When a worker is given a job, it is given an exclusive lock to that job. That means
that job won't be given to any other worker, so long as the worker checks in with
progress on the job. By default, jobs have to either report back progress every 60
seconds, or complete it, but that's a configurable option. For longer jobs, this 
may not make sense.

	# Hooray! We've got a piece of work!
	job = queue.pop
	# How long until I have to check in?
	job.remaining
	# => 59
	# Hey! I'm still working on it!
	queue.heartbeat(job)
	# => 1331326141.0
	# Ok, I've got some more time. Oh! Now I'm done!
	queue.complete(job)

One nice feature of `qless` is that you can get statistics about usage. Stats are
binned by day, so when you want stats about a queue, you need to say what queue
and what day you're talking about. By default, you just get the stats for today.
These stats include information about the mean job wait time, standard deviation,
and histogram. This same data is also provided for job completion:

	# So, how're we doing today?
	stats = client.stats.get('testing')
	# => { 'run' => {'mean' => ..., }, 'wait' => {'mean' => ..., }}

Benchmarks
==========

This is an example run of `qless` using the python bindings. It's designed to test
only the queueing mechanism (each job is a no-op). There's a parameter to turn up
or down the portion of jobs that each worker drops on the floor. With all jobs 
getting finished, the numbers look good (these were conducted on my 2011-ish MacBook
Pro with Redis 2.9.5):

	$ ./forgetful-bench.py --jobs 10000 --workers 50 --forgetfulness 0 --quiet
	Wait:
		Count: 10000
		Mean : 3.098267s
		SDev : 0.665414
		Wait Time Histogram:
			 0, 0.026400000, 264
			 1, 0.432700000, 4327
			 2, 0.427700000, 4277
			 3, 0.113200000, 1132
	Run:
		Count: 10000
		Mean : 0.006937s
		SDev : 0.006958
		Completion Time Histogram:
			 0, 1.000000000, 10000
	==================================================
	Put jobs : 1.923535s
	Do jobs  : 4.363685s
	Redis Mem: 18.00M
	Redis Lua: 81920
	Redis CPU: 4.540000s
	Flushing

Even with substantial forgetfulness, the numbers _still_ look good. In this test, each
worker completely drops 30% of the jobs it's given. And yet, every single job completes.

	$ ./forgetful-bench.py --jobs 10000 --workers 50 --forgetfulness 0.3 --quiet
	Wait:
		Count: 14299
		Mean : 3.810464s
		SDev : 1.243206
		Wait Time Histogram:
			 0, 0.014826212, 212
			 1, 0.277501923, 3968
			 2, 0.279460102, 3996
			 3, 0.271207777, 3878
			 4, 0.108678929, 1554
			 5, 0.032869431, 470
			 6, 0.011049724, 158
			 7, 0.003286943, 47
			 8, 0.000979089, 14
			 9, 0.000139870, 2
	Run:
		Count: 10000
		Mean : 0.006961s
		SDev : 0.007127
		Completion Time Histogram:
			 0, 1.000000000, 10000
	==================================================
	Put jobs : 1.945180s
	Do jobs  : 10.110526s
	Redis Mem: 18.00M
	Redis Lua: 78848
	Redis CPU: 5.610000s
	Flushing

The most I've abused the system thus far is with 1M jobs, and 10% forgetfulness. It
took about 10 minutes, and again did not lose a single job. I would be happy to test
it more, but I frankly just get tired of waiting.

Coming-Soon Features
====================

The features that are highest priority for me at the moment are:

1. __Dropped Job Backoff__ -- We came to the concensus that we should probably 
	re-schedule jobs that get dropped on the floor to avoid transient failures. Phil
	suggested constant backoff since we're likely dealing with jobs on a larger
	granularity than sub-second.
1. __Job Type__ -- Each job should be accompanied with a job type. This is something
	that `resque` does and their clients leverage it. __It is up to the consumer /
	client library to determine how to use it.__ For example, the Ruby client might
	choose to interpret it as a class identifier, but the C client might handle it
	in a different way.
1. __Web App__ -- With the advent of a Ruby client, I want to advance the state of
	the web interface significantly.
1. __Max Jobs Per Host__ -- It would also be nice if you could decide how many jobs
	of a certain kind that a host can have out at any one time.
1. __Archival of Jobs__ -- Brandon suggested the possibility of archiving completed
	jobs to permanent storage. This might be nice, but I think it might be best 
	to leave this as the responsibility of clients. It's also possible that there
	just be a consumer helper that archives to S3, disk, etc.
1. __Host Throttling__ -- Brandon had a really good suggestion that perhaps hosts
	should be throttled based on how many jobs that they drop or fail. The suggestion
	was that we could track statistics about how many jobs get dropped by all hosts
	and then mark a host as 'bad' when it is sufficiently faily. I'm planning on
	reading up more on failure analysis.
1. __Per-Queue / Per-Type Throttling__ -- Myron really wants to have support for
	only allowing a certain number of jobs of a certain type out at any one time.
	I'd be amenable to this, and we can look at how we want to implement it.

Remaining Questions
===================

1. __Sub Queueing for Large Numbers of Jobs__ -- Phil suggested that depending on
	the performance profile, it might be worth subqueueing large queues to avoid the
	penalty of maintaining a sorted set. I'm working on some benchmarks to help
	inform the decision.
1. __Data Cleanup__ -- Certain pieces of data are amenable to being cleand up as
	we go. Jobs, for instance. Whenever a job is completed, we can delete any jobs
	whose data is expired. But what of our list of workers? What about statistics?
	Should we have an admin operation that's along the lines of `clean-stale-data`?
	Phil suggested a nanny client, which could be a good solution to lot of problems
1. __Blocking Pop__ -- Phil thiks that this is important, depending on how many jobs
	we're expecting to come through, how often, etc. I'd like to follow up with 
	him to make sure that this is necessary, though.
