/*
 * This bit of code describes some of the semantics of a queue,
 * including pushing, popping, etc. It knows about locks, expirations,
 * and so forth, and abstracts the underlying implementation.
 */

var uuid   = require('node-uuid');
var redis  = require('redis');
var client = redis.createClient();

/* This is the prefix applied to all queue names */
var prefix = 'qless:'

/* Make a unique identifier */
var makeId = function() {
	return uuid.v4();
}

function Queue(name, heartbeat, retries) {
	this.name      = prefix + name;
	this.heartbeat = heartbeat;
}

Queue.prototype.renew = function(items, callback) {
	var m = client.multi();
	var expiration = new Date().getTime() + this.heartbeat;
	for (var index in items) {
		m.zadd(this.name + '-pending', expiration, items[index]);
	}
	m.exec(callback);
}

Queue.prototype.pop = function(count, callback) {
	var c = count || 1;
	/* First things first, check the locks */
	client.zrangebyscore(this.name + '-pending', -10000000000000, threshold, count, function(err, results) {
		if (err) {
			return callback(err, null);
		}
		
		var expiration = new Date().getTime() + this.heartbeat;
		
		/* Extend the locks for these items */
		var m = client.multi();
		for (var index in results) {
			m.zadd(this.name + '-pending', expiration, results[index]);
		}
		m.exec(function() {});
		
		/* Now, we'll make sure we have a full number of objects requested */
		count -= results.length;
		if (count) {
			/* Grab the next batch requested */
			var m = client.multi();
			m.zrange(this.name, 0, count - 1);
			m.zremrangebyrank(this.name, 0, count - 1);
			m.exec(function(e, r) {
				results = results.concat(r);
				/* With these in hand, make locks for these items */
				var m = client.multi();
				for (var i in r) {
					m.zadd(this.name + '-pending', expiration, r[i]);
				}
				m.exec(function() {
					callback(err, results);
				});
			});
		} else {
			callback(err, results);
		}
	});
}

Queue.prototype.push = function(items, callback) {
	/* If it's an array, then it's assumed to be an array of items to push */
	if (items instanceof Array) {
		var m = client.multi();
		for (var index in items) {
			m.zadd(this.name, new Date().getTime(), items[index].id || makeId());
		}
		m.exec(function(err, replies) {
			callback(err, replies);
		});
	} else {
		console.log(items);
		client.zadd(this.name, new Date().getTime(), items.id || makeId(), callback);
	}
}

exports.Queue = Queue;

exports.list = function(callback) {
	client.keys('qless:*', function(err, results) {
		var m = client.multi();
		for (var index in results) {
			m.zcard(results[index]);
		}
		var r = {};
		m.exec(function(e, lengths) {
			for (var index in lengths) {
				r[results[index]] = lengths[index];
			}
			callback(r);
		});
	});
}