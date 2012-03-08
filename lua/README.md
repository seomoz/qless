API
===

Here is some brief documentation of how the lua scripts work and what they expect.
Each invocation begins with a number, which describes how many of the provided
values are considered `KEYS`, as they refer in some sense to a Redis key. The
remaining values are considered `ARGV`. This is a distinction that Redis makes
internally, and should be considered a magic number.

Common Arguments
----------------

All times are specified as UTC timestamps. I imagine this is something that might
become somewhat contentious. Lua scripts are not allowed access to the system
clock. They actually have a pretty good reason for that, but it also means that
each client must provide their own times. This has the side effect of requiring
that all clients have relatively __synchronized clocks__.

- `id` -- the id of the job, a hexadecimal uuid
- `data` -- a JSON blob representing the user data associated with a job
- `queue` -- the name of a queue
- `worker` -- a unique string identifying a process on a host

SetConfig(0, option, [value])
-----------------------------
Set the configuration value for the provided option. If `value` is omitted,
then it will remove that configuration option.
__Returns__: nothing

GetConfig(0, [option])
----------------------
Get the current configuration value for that option, or if option is omitted,
then get all the configuration values. __Returns__: The value of the option

Heartbeat(0, id, worker, expiration, [data])
-------------------------------------------
Renew the heartbeat, if possible, and optionally update the job's user data.
__Returns__: a JSON blob with `False` if the job was not renewed, or the
updated expiration time

Put(1, queue, id, data, now, [priority, [tags, [delay]]])
---------------------------------------------------------
Either create a new job in the provided queue with the provided attributes,
or move that job into that queue. If the job is being serviced by a worker,
subsequent attempts by that worker to either `heartbeat` or `complete` the
job should fail and return `false`.

The `priority` argument should be negative to be run sooner rather than 
later, and positive if it's less important. The `tags` argument should be
a JSON array of the tags associated with the instance and the `delay`
argument should be in how many seconds the instance should be considered 
actionable.

__Returns__: The id of the put job, or raises an error on failure

Get(0, id)
----------
Get the data associated with a job. __Returns__: JSON blob describing the
job.

Pop(1, queue, worker, count, now, expiration)
---------------------------------------------
Passing in the queue from which to pull items, the current time, when the locks
for these returned items should expire, and the number of items to be popped
off.

Peek(1, queue, count, now)
--------------------------
Similar to the `Pop` command, except that it merely peeks at the next items
in the queue.

Cancel(0, id)
-------------
Cancel a job from taking place. It will be deleted from the system, and any
attempts to renew a heartbeat will fail, and any attempts to complete it
will fail. If you try to get the data on the object, you will get nothing.

Complete(0, id, worker, queue, now, [data, [next, [delay]]])
------------------------------------------------------------
Complete a job and optionally put it in another queue, either scheduled or to
be considered waiting immediately. __Returns__: The updated state, or False
on error

Fail(0, id, worker, type, message, now, [data])
-----------------------------------------------
Mark the particular job as failed, with the provided type, and a more specific
message. By `type`, we mean some phrase that might be one of several categorical
modes of failure. The `message` is something more job-specific, like perhaps
a traceback.

This method should __not__ be used to note that a job has been dropped or has 
failed in a transient way. This method __should__ be used to note that a job has
something really wrong with it that must be remedied.

The motivation behind the `type` is so that similar errors can be grouped together.
Optionally, updated data can be provided for the job. A job in any state can be
marked as failed. If it has been given to a worker as a job, then its subsequent
requests to heartbeat or complete that job will fail. Failed jobs are kept until
they are canceled or completed. __Returns__ the id of the failed job if successful,
or `False` on failure.

Failed(0, [type, [start, [limit]]])
-----------------------------------
If no type is provided, this returns a JSON blob of the counts of the various
types of failures known. If a type is provided, it will report up to `limit`
from `start` of the jobs affected by that issue. __Returns__ a JSON blob.

	# If no type, then...
	{
		'type1': 1,
		'type2': 5,
		...
	}
	
	# If a type is provided, then...
	{
		'total': 100,
		'jobs': [
			{
				# All the normal keys for a job
				'id': ...,
				'data': ...
				# The message for this particular instance
				'message': ...,
				'type': ...,
			}, ...
		]
	}

Stats(0, queue, date)
---------------------
Return the current statistics for a given queue on a given date. The results 
are returned are a JSON blob:

	{
		# These are unimplemented as of yet
		# 'failed': 3,
		# 'retries': 5,
		'wait' : {
			'total'    : ...,
			'mean'     : ...,
			'variance' : ...,
			'histogram': [
				...
			]
		}, 'run': {
			'total'    : ...,
			'mean'     : ...,
			'variance' : ...,
			'histogram': [
				...
			]
		}
	}

The histogram's data points are at the second resolution for the first minute,
the minute resolution for the first hour, the 15-minute resolution for the first
day, the hour resolution for the first 3 days, and then at the day resolution
from there on out. The `histogram` key is a list of those values.

ConsistencyCheck(0, [resolve])
------------------------------
__Unimplemented__ This is designed to look at the current state of the redis 
instance and report any inconsistencies found in the keys supporting `qless`.
If `resolve` is passed in as anything other than `nil`, then it will also 
attempt to resolve these inconsistencies.




Job Structure
=============

Jobs are stored in a key `ql:j:<id>`, and have several important keys:

	{
		# This is the same id as identifies it in the key. It should be
		# a hex value of a uuid
		'id'        : 'deadbeef...',
		
		# This is the priority of the job -- lower means more priority.
		# The default is 0
		'priority'  : 0,
		
		# This is the user data associated with the job. (JSON blob)
		'data'      : '{"hello": "how are you"}',
		
		# A JSON array of tags associated with this job
		'tags'      : '["testing", "experimental"]',
		
		# The worker ID of the worker that owns it. Currently the worker
		# id is <hostname>-<pid>
		'worker'    : 'ec2-...-4925',
		
		# This is the time when it must next check in
		'expires'   : 1352375209,
		
		# The current state of the job: 'waiting', 'pending', 'complete'
		'state'     : 'waiting',
		
		# The queue that it's associated with. 'null' if complete
		'queue'     : 'example',
		
		# A list of all the stages that this node has gone through, and
		# when it was put in that queue, given to a worker, which worker,
		# and when it was completed. (JSON blob)
		'history'   : [
			{
				'q'     : 'test1',
				'put'   : 1352075209,
				'popped': 1352075300,
				'done'  : 1352076000,
				'worker': 'some-hostname-pid'
			}, {
				...
			}
		]
	}

Configuration Options
=====================

The configuration should go in the key `ql:config`, and here are some of the
configuration options that `qless` is meant to support:

1. `heartbeat` (60) --
	The default heartbeat in seconds for implicitly-created queues
1. `stats-history` (30) --
	The number of days to store summary stats
1. `histogram-history` (7) --
	The number of days to store histogram data
1. `jobs-history-count` (50k) --
	How many jobs to keep data for after they're completed
1. `jobs-history` (7 * 24 * 60 * 60) --
	How many seconds to keep jobs after they're completed

Queues
======

A queue is a priority queue and consists of three parts:

1. `ql:q:<name>-scheduled` -- a sorted list of all scheduled job ids
1. `ql:q:<name>-work` -- a sorted list (by priority) of all jobs waiting
1. `ql:q:<name>-locks` -- a sorted list of job locks and expirations

When looking for a unit of work, the client should first choose from the 
next expired lock. If none are expired, then we should next make sure that
any jobs that should now be considered eligible (the scheduled time is in
the past) are then inserted into the work queue.

Locking
=======

A worker is given an exclusive lock on a piece of work when it is given
that piece of work. That lock may be renewed periodically so long as it's
before the provided 'heartbeat' timestamp. Likewise, it may be completed.

If a worker attempts to heartbeat a job, it may optionally provide an updated
JSON blob to describe the job. If the job has been given to another worker,
the heartbeat should return `false` and the worker should yield.

When a node attempts to heartbeat, the lua script should check to see if the
node attempting to renew the lock is the same node that currently owns the
lock. If so, then the lock's expiration should be pushed back accordingly, 
and the updated expiration returned. If not, it only has to return false.

Stats
=====

I'm planning on collecting statistics for job wait time (time popped - time put),
job completion time (time completed - time popped). By 'statistics', I mean
average, variance, count and a histogram. These stats will be binned by `queue`,
the day of completion (for completion time) and the day a job was popped (for wait
time). I also plan on binning this data by `tag` eventually, as well as `worker`,
but those are down the road once I can get a feel for some of these stats.

__Update__ Because there is currently no support for date formatting in the Redis
Lua scripts, we're going to take a slightly different approach. These stats will
be binned by day still, but the key will not be human-readable. Instead,

	# This maps the current time to midnight of that day's timestamp
	<day> = time - (time % (24 * 60 * 60))

Stats will be stored under keys of the form `ql:s:wait:<day>:<qname>[:<tag>]`
for the time spent waiting to get given to a worker and
`ql:s:run:<day>:<qname>[:<tag>]`. These keys will store hashes with the keys:

- `total` -- The total number of data points contained
- `mean` -- The current mean value
- `vk` -- Not the actual variance, but a number that can be used to both numerically
	stable-ly find the variance, and compute it in a
	[streaming fashion](http://www.johndcook.com/standard_deviation.html)
- `s1`, `s2`, ..., -- second-resolution histogram counts for the first minute
- `m1`, `m2`, ..., -- minute-resolution for the first hour
- `h1`, `h2`, ..., -- hour-resolution for the first day
- `d1`, `d2`, ..., -- day-resolution for the rest

There will also be another key, a hash, `ql:s:stats:<day>:<qname>` which some of
the other summary values:

- `failures` -- This is how many failures there have been. If a job is run twice
	and fails repeatedly, this is incremented twice.
- `failed`   -- This is how many are currently failed
- `retries`  -- This is how many jobs we've had to retry

Failures
========

Failures should be stored in such a way that we can quickly summarize the number
of failures of a given type, but also which items have succumb to that type of 
failure. With that in mind, I propose a set `ql:failures` whose members are the
names of the various failure lists. Each type of failure then has its own list of
instance ids that encountered such a failure. For example, we might have:

	ql:failures
	=============
	upload error
	widget failure
	
	ql:f:upload error
	==================
	deadbeef
	...

Job Data Deletion
=================

We should delete data about completed jobs periodically. We should prune both
by the policies for the maximum number of retained completed jobs, and by the
maximum age for retained jobs. To accomplish this, we'll use a sorted list to
keep track of which items should be expired. This list should be stored in the
key `ql:completed`














