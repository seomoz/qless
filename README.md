qless
=====

Qless is a `Redis`-based job queueing system inspired by `resque`, but built on
a collection of Lua scripts, maintained in the [qless-core](https://github.com/seomoz/qless-core)
repo.

Philosophy
==========

A `job` is a unit of work. A `queue` can contain several jobs that are scheduled to
be run at a certain time, several jobs that are waiting to run, and jobs that are
currently running. A `worker` is a process on a host, identified uniquely, that asks
for jobs from the queue, performs some process associated with that job, and then
marks it as complete. When it's completed, it can be put into another queue.

Jobs can only be in one queue at a time. That queue is whatever queue they were last
put in. So if a worker is working on a job, and you move it, the worker's request to
complete the job will be ignored.

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

1. __Job Type__ -- Each job should be accompanied with a job type. This is something
	that `resque` does and their clients leverage it.
1. __Retry logic__ -- Each job should have a number of retries associated with it. 
	These retries should be considered renewed upon successful completion of a stage.
	In other words, if a job has 3 retries, and it fails 2 times in queue `A` before
	completing, then when it gets to queue `B`, it will have 3 more retries.
1. __Tagging / Tracking__ -- Users should be able to identify jobs that they would
	like to track the progress of. This is likely no more than just listing the current
	state and history of those jobs. I have made an ad-hoc system for this for myself,
	and it has been incredibly useful and I use it every day.
1. __Web App__ -- With the advent of a Ruby client, I want to advance the state of
	the web interface significantly.
