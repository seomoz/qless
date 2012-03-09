qless
=====

My hope for qless is that it will make certain aspects of pipeline management will be made
easier. For the moment, this is a stream of consciousness document meant to capture the 
features that have been occurring to me lately. After these, I have some initial thoughts
on the implementation, concluding with the outstanding __questions__ I have.

I welcome input on any of this.

Context
-------

This is a subject that has been on my mind in particular in three contexts:

1. `custom crawl` -- queue management has always been an annoyance, and it's reaching the
	breaking point for me
1. `freshscape` -- I'm going to be encountering very similar problems like these in freshscape,
	and I'd like to be able to avoid some of the difficulties I've encountered.
1. `general` -- There are a lot of contexts in which such a system would be useful.
 	__Update__ Myron pointed out that in fact `resque` is built on a simple protocol,
	where each job is a JSON blob with two keys: `id` and `args`. That makes me feel
	like this is on the right track!

Feature Requests
----------------

Some of the features that I'm really after include

1. __Jobs should not get dropped on the floor__ -- This has been a problem for certain 
	projects, including our custom crawler. In this case, jobs had a propensity for 
	getting lost in the shuffle.
1. __Job stats should be available__ -- It would be nice to be able to track summary statistics
	in one place. Perhaps about the number currently in each stage, waiting for each stage,
	time spent in each stage, number of retries, etc.
1. __Job movement should be atomic__ -- One of the problems we've encountered with using
	Redis is that it's been hard to keep items moving from one queue to another in an atomic
	way. This has the unfortunate effect of making it difficult to trust the queues to hold
	any real meaning. For example, the queues use both a list and a hash to track items, and
	the lengths of the two often get out of sync.
1. __Retry logic__ -- For this, I believe we need the ability to support some automatic 
	retry logic. This should be configurable, and based on the stage
1. __Data lookups should be easy__ -- It's been difficult to quickly identify a work item and
	get information on its state. We've usually had to rely on an external storage for this.
1. __Manual requeuing__ -- We should be able to safely and atomically move items from one
	queue into another. We've had problems with race conditions in the past.
1. __Priority__ -- Jobs should be describable with priority as well. On occasion we've had
	to push items through more quickly than others, and it would be great if the underlying
	system supported that.
1. __Tagging / Tracking__ -- It would be nice to be able to mark certain jobs with tags, and
	as work items that we'd like to track. It should then be possible to get a summary along
	the lines of "This is the state of the jobs you said you were interested in." I have a
	system for this set up for my personally, and it has been _extremely_ useful.
1. __The system should be reliable and highly available__ -- We're trusting this system to
	be a single point of failure, and as such it needs to be robust and dependable.
1. __High Performance__ -- We should be able to expect this system to support a large number
	of jobs in a short amount of time. For some context, we need custom crawler to support 
	about 50k state transitions in a day, but my goal is to support millions of transitions
	in a day, and my bonus goal is 10 million or so transitions in a day.
1. __Scheduled Work__ -- We should be able to schedule work items to be enqueued as some 
	specified time.
1. __UI__ -- It would be nice to have a useful interface providing insight into the state of
	the pipeline(s).
1. __Namespaces__ -- It might be nice to be able to segment the jobs into namespaces based on
	project, stage, type, etc. It shouldn't have any explicit meaning outside of partitioning
	the work space.
1. __Language Agnosticism__ -- The lingua franca for this should be something supported by a
	large number of languages, and the interface should likewise be supported by a large number
	of languages. In this way, I'd like it to be possible that a job is handled by one language
	in one stage, and conceivably another in a different stage.
1. __Clients Should be Easy to Write__ -- I don't want to put too much burden on the authors
	of various clients, because I think this helps a project to gain adoption. But, the 
	out-of-the-box feature set should be compelling.

Thoughts / Recommendations
--------------------------

1. `Redis` as the storage engine. It's been heavily battle-tested, and it's highly available, 
	supports much of the data structures we'd need for these features (atomicity, priority,
	robustness, performance, replicatable, good at saving state). To boot, it's widely available.
1. `JSON` as the lingua franca for communication of work units. Every language I've encountered
	has strong support for it, it's expressive, and it's human readable.

Until recently, I had been imagining an HTTP server sitting in front of Redis. Mostly because
I figured that would be one way to make clients easy to write -- if all the logic is pinned
up in the server. That said, it's just a second moving part upon which to rely. And Myron
made the very compelling case for having the clients maintain the state and rely solely on 
Redis. However, Redis 2.6 provides us with a way to get the best of both worlds -- clients
that are easy to write and yet smart enough to do everything themselves. This mechanism is
stored Lua scripts.

Redis has very tight integration with scripting language `Lua`, and the two major selling points
for us on this point are:

1. Atomicity _and_ performance -- Lua scripts are guaranteed to be the only thing running 
	on the Redis instance, and so it makes certain locking mechanisms significantly easier.
	And since it's running on the actual redis instance, we can enjoy great performance.
1. All clients can use the same scripts -- Lua scripts for redis are loaded into the instance,
	and then can be identified by a hash. But the language invoking them is irrelevant. As such,
	the burden can still be placed on the original implementation, clients can be easy to
	write, but still be smart enough to manage the queues themselves.

One other added benefit is that when using Lua, redis imports a C implementation of a JSON
parser and makes it available from Lua scripts. This is just icing on the cake.

Planned Features / Organization
===============================

All the smarts are essentially going to go into a collection of Lua scripts to be stored and
run on Redis. In addition to these Lua scripts, I'd like to provide a simple web interface
to provide pretty access to some of this functionality.

Round 1
-------

1. __Workers must heartbeat jobs__ -- When a worker is given a job, it is given an exclusive
	lock, and no other worker will get that job so long as it continues to heartbeat. The 
	service keeps track of which locks are going to expire, and will give the work to another
	worker if the original worker fails to check in. The expiry time is provided every time
	work is given to a worker, and an updated time is provided when heartbeat-ing. If the
	lock has been given to another worker, the heartbeat will return `false`.
1. __Stats A-Plenty__ -- Stats will be kept of when a job was enqueued for a stage, when it
	was popped of to be worked on, and when it was completed. In addition, summary statistics
	will be kept for all the stages.
1. __Job Data Stored Temporarily__ -- The data for each job will be stored temporarily. It's
	yet to be determined exactly what the expiration policy will be, (either the last _k_
	jobs or the last _x_ amount of time). But still, all the data about a job will be available
1. __Atomic Requeueing__ -- If a work item is moved from one queue to another, it is moved.
	If a worker is in the middle of processing it, its heartbeat will not be renewed, and it
	will not be allowed to complete.
1. __Scheduling / Priority__ -- Jobs can be scheduled to become active at a certain time.
	This does not mean that the job will be worked on at that time, though. It simply means
	that after a given scheduled time, it will be considered a candidate, and it will still
	be subject to the normal priority rules. Priority is always given to an active job with
	the lowest priority score.
1. __Tracking__ -- Jobs can be marked as being of particular interest, and their progress
	can be tracked accordingly.
1. __Web App__ -- A simple web app would be nice

Round 2
-------

1. __Retry logic__ -- For this, I believe we need the ability to support some automatic 
	retry logic. This should be configurable, and based on the stage
1. __Tagging__ -- Jobs should be able to be tagged with certain meaningful flags, like a 
	version number for the software used to process it, or the workers used to process it.

Questions
=========
	
1. __Implicit Queue Creation__ -- Each queue needs some configuration, like the heartbeat rate,
	the time to live for a job, etc. And not only that, but there might be additional more complicated
	configuration (flow of control). So, which of these should be supported and which not?
	
	1. Static queue definition -- at a very minimum, we should be able to configure some ahead of time
	1. Dynamic queue creation -- should there just be another endpoint that allows queues to be added?
		If so, should these queues then be saved to persist?
	1. Implicit queue creation -- if we push to a non-existent queue, should we get a warning? 
		An error? Should the queue just be created with some sort of default values?
	
	On the one hand, I would like to make the system very flexible and amenable to sort of 
	ad-hoc queues, but on the other hand, there may be non-default-able configuration values
	for queues.

1. __Job Data Storage__ -- How long should we keep the data about jobs around? We'd like to be
	able to get information about a job, but those should probably be expired. Should expiration
	policy be set to hold jobs for a certain amount of time? Should this window be configured for
	simply the last _k_ jobs?
