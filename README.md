qless
=====

My hope for qless is that it will make certain aspects of pipeline management will be made
easier. For the moment, this is a stream of consciousness document meant to capture the 
features that have been occurring to me lately.

This is a subject that has been on my mind in particular in three contexts:

1. `custom crawl` -- queue management has always been an annoyance, and it's reaching the
	breaking point for me
1. `freshscape` -- I'm going to be encountering very similar problems like these in freshscape,
	and I'd like to be able to avoid some of the difficulties I've encountered.
1. `general` -- There are a lot of contexts in which such a system would be useful. There
	is, of course, `resque` for Ruby fanboys, but I believe this breaks off important
	features. Most notably, language agnosticism.

Specifically,

1. __Jobs should not get dropped on the floor__ -- This has been a problem for certain 
	projects, including our custom crawler. In this case, jobs had a propensity for 
	getting lost in the shuffle. As such, __heartbeats__ should be optionally made required
	for a queue, so that client libraries are not responsible for both queue management
	and for making sure that they never fail.
1. __Jobs should be trackable__ -- It would be nice to be able to track summary statistics
	in one place. Perhaps about the number currently in each stage, waiting for each stage,
	time spent in each stage, number of retries, etc.
1. __Job movement should be atomic__ -- One of the problems we've encountered with using
	Redis is that it's been hard to keep items moving from one queue to another in an atomic
	way. This has the unfortunate effect of making it difficult to trust the queues to hold
	any real meaning. For example, the queues use both a list and a hash to track items, and
	the lengths of the two often get out of sync.
1. __Customizable pipelines__ -- __UNDECIDED__ Work items ought to be able to indicate the
	stages they need to have happen to them
1. __Retry logic__ -- For this, I believe we need the ability to support some automatic 
	retry logic. This should be configurable, and based on the stage
1. __Data lookups should be easy__ -- It's been difficult to quickly identify a work item and
	get information on its state. We've usually had to rely on an external storage for this.
1. __Manual requeuing__ -- We should be able to safely and atomically move items from one
	queue into another. We've had problems with race conditions in the past.
1. __Priority__ -- Jobs should be describable with priority as well. On occasion we've ha
	to push items through more quickly than others, and it would be great if the underlying
	system supported that.
1. __Tagging / Tracking__ -- It would be nice to be able to mark certain jobs with tags, and
	as work items that we'd like to track. It should then be possible to get a summary along
	the lines of "This is the state of the jobs you said you were interested in." I have a
	system for this set up for my personally, and it has been _extremely_ useful.
1. __The system should be reliable and highly available__ -- We're trusting this system to
	be a single point of failure, and as such it needs to be robust and dependable. To this
	end, I recommend that the state be stored in a persistent, replicable store, like Redis.
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
1. __Modifiers__ -- __UNDECIDED__ We need some mechanism for workers to report some modifiers 
	to describe themselves and provide some insight into what kind of work units that should
	be given to them. A better mechanism for this might be just queue names, which is what we're
	currently using in custom crawl.
1. __Language Agnosticism__ -- The lingua franca for this should be something supported by a
	large number of languages, and the interface should likewise be supported by a large number
	of languages. In this way, I'd like it to be possible that a job is handled by one language
	in one stage, and conceivably another in a different stage.

With these goals in mind, I have some preliminary recommendations:

1. `Redis` as the storage engine. It's been heavily battle-tested, and it's highly available, 
	supports much of the data structures we'd need for these features (atomicity, priority,
	robustness, performance, replicatable, good at saving state). To boot, it's widely available.
1. `HTTP` as the communication mechanism. A large number of languages support HTTP, and it has
	a lot of semantic meaning that would be conducive here -- `GET`, `PUT` and `POST` for example,
	and also would be conducive to building a simple user interface.
1. `JSON` as the lingua franca for communication of work units. Every language I've encountered
	has strong support for it, it's expressive, and it's human readable.
1. `node.js` as the server language. This is less necessary, so long as the interface is maintained,
	but it's been a bit of a heart-throb recently for me, and it is definitely meant to support
	the scale and throughput requirements we have. And, as it's JavaScript-based, it would be
	conducive to working with JSON as a lingua franca.