qless API
=========

This document aims to describe the interface of qless from a high level to the API level.

Essentially, `jobs` move through various `queues` or `stages`, where workers request units
of work from a specific queue and upon completion turn that piece of work back in.

Jobs
----

A `job` is a unit of work that may move through several stages. Every job is uniquely
identified by a unique hexidecimcal id. This id is left explicitly vague so that it can
be left up to the client to provide a hash that uniquely identifies either a work unit, 
or work configuration (for example, a user might want to avoid repeatedly performing
essentially identical jobs).

A job may only appear in one stage or queue at a time.

A job may have metadata that is used during the course of each of the stages of execution.
This metadata may be updated when the job is returned to the system. This provides a simple
message-passing interface between stages. This should not be used for high bandwidth, but
might provide a reference to an external source of data (S3, URL, etc.). `qless` only seeks
to deal with the orchestration of these stages, and not the details of their data.

Stage
-----

The words `stage` and `queue` are used somewhat interchangeably here. They both describe
a process that is applied to a job. The inputs and outputs for these stages are beyond the
scope of `qless` -- do what you will with them.

Heartbeats
----------

Some stages might require a heartbeat to maintain a lock on a unit of work. This mechanism
can be utilized to ensure that units of work are never forgotten or dropped on the floor,
and in the case of catastrophic worker failure, to reduce the ultimate latency for the job
getting completed. In particular, if a stage can be very long and involved, the heartbeat 
is a timescale that can be much shorter, so that failures can be detected on a shorter 
time scale than the lifetime of a stage.

HTTP Interface
==============

All responses from the server are dictionaries, where there is at minimum a key `results`,
which describes the response to the request. Other keys may appear, to include performance
stats, like the time taken to serve a request. Future metadata may be added here, but from
here on out, any reference to the `response` or `results` refers to the `results` key of 
the response dictionary. In the event of an error, however, the `results` key is `null`, 
and the `error` key describes the error that occurred. In general, however, we will try
to obey HTTP semantics and make use of status codes other than 200 to convey errors. For
example:

	{
		"results": {
			# This would be the response to your query
		}, "time": {
			# This would be considered metadata
			"total": 5
		}, "load": {
			# And so would this
			"cpu"   : 0.52,
			"memory": 152
		}
	}

Any data conveyed to the HTTP interface (through `PUT` or `POST`) must be a `application/json`
encoded dictionary, including at minimum the `data` key, the data to be applied to the 
endpoint. Optionally, the `host` key may also appear to identify the host for statistics
purposes. In the absence of this key, it will be assumed to be the IP address of the sender.

Getting Work
------------

Work units are obtained through a `GET` request to an endpoint naming a queue, and optionally
the number of elements that should be retrieved. The general format is `/get/<queue>/<count?>`. For
example, this would request 5 items from the `crawl` queue:

	GET /crawl/5

The output is encoded in `application/json`, and describes the requested work items. If a 
single item is requested, the result is simply a dictionary describing that item. If more
than one item was requested, then the result is an array of such dictionaries. At minimum,
the `meta`, `id` and `queue` keys will appear in each work item. The `meta` key describes the
editable metadata associated with the job. The `id` is the hexidecimal hash that uniquely
identifies the work item. The `queue` simply mentions the queue from which this item was
pulled. Momentarily we'll describe the additional keys that may appear, but in the mean time,
an example result might be:

	# GET /get/work/2
	{
		"results": [{
			"meta" : "foo",
			"id"   : "deadbeef1",
			"queue": "work"
		}, {
			"meta" : {
				"name": "howdy"
			},
			"id"   : "aed292fed",
			"queue": "work"
		}]
	}

Additional keys that may be included with a work unit, include the following. Any additional
keys that appear in a result should be considered undefined and thus ignored.

1. `heartbeat` (int) -- the time in seconds before which the client must 'heartbeat' the 
	request before the exclusive lock on that work item is awarded to someone else. For example,
	a queue that requires a heartbeat every 5 minutes would specify '300'
1. `cutoff`    (int) -- the time in seconds that this particular stage is allowed before it
	will be considered dead. Clients might make use of this to preempt processes that could
	otherwise take indefinitely long and submit any intermediate results.

Heartbeat
---------

Heartbeats must be conveyed over a `POST` request to the `/heartbeat/<queue>` endpoint, and
must include an array of work unit ids for which to renew the hearbeat. The response contains
an array of the time needed until the next heartbeat for each of the supplied work items, or
`false` if the exclusive lock for that item has been locked and that work item is now with 
another worker. For example, an example conversation might go something like this:

	# POST /hearbeat/work
	{
		"host": "worker-1",
		"data": [
			"aed292fed",
			"deadbeef1"
		]
	}
	
	# Response
	{
		# This means that one lock was renewed for 5 minutes but the other has been lost
		"results" : [
			300,
			false
		]
	}

Completing Work
---------------

Work units are completed by a `POST` to the endpoint `/complete/<queue>`, in an array. Each
completed work unit must be accompanied by the editable metadata for the work unit, and its
id. The response is an array of the ids that were marked as complete. For example, to
complete the above examples:

	# POST /complete/work
	{
		"host": "worker-1",
		"data": [{
			"meta": ...,
			"id"  : "deadbeef1"
		}, {
			"meta": ...,
			"id"  : "aed292fed"
		}]
	}
	
	# Response
	{
		"results": [
			"deadbeef1",
			"aed292fed"
		]
	}

Adding Work
-----------

This whole system would be relatively useless if you couldn't add jobs in the first place.
You can accomplish this with a `POST` to the endpoint `/add/<queue>`, posting an array of 
the work units you would like to add. Each work item must at least contain the `meta` key,
which describes the job, and optionally an `id`. If no `id` is provided, then a unique 
identifier will be provided. The response is an array of all the `id`s inserted. In the 
event that a work unit cannot be inserted, `false` will be returned. An example conversation
might go something like this:

	# POST /add/work
	{
		"host": "worker-1",
		"data": [{
			"meta": "foo",
			"id"  : "deedee"
		}, {
			# An id will be automatically assigned
			"meta": "bar"
		}]
	}
	
	# Response
	{
		"results": [
			"deedee",
			"added1"
		]
	}

Additional Endpoints
====================

There are some additional endpoints that might be helpful for you.

1. `/queues` -- this lists all queues that `qless` knows about, and some stats about each,
	including the number enqueued for that stage and the number currently being worked on
	in that stage.
2. `/job/<id>` -- this provides information about the particular job, including the current
	state of the job ('waiting', 'pending', or 'complete'), the stage it's waiting for (if
	any), and its metadata.

User Interface
==============

At this point, the UI is admittedly limited, but it's meant to grow. To access it, however,
you should visit `/ui/`.














