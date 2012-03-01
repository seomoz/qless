/*
 * This is the implementation of the qless server. The API for this server
 * is described in the README in directory.
 */

var express  = require('express');
var queue    = require('./queue.js');

var app = express.createServer();

app.configure(function() {
	/* This is so that things sent a JSON will already be decoded 
	 * when we get access to them in req.body */
	app.use(express.bodyParser());
	
	app.use(express.errorHandler());
	
	/* This is to set up hamljs */
	app.set('views', __dirname + '/templates');
	app.set('view options', {layout: false});
	app.register('.haml', require('hamljs'));
	
	/* This is any static content we'd like to serve */
	app.use(express.static(__dirname + '/static', { maxAge: 1000 * 3600 * 24 * 7 }));
});

/*
 * This is the endpoint for getting one or more work items
 */
app.get('/get/:queue/:count?', function(req, res) {
	if (req.params.count) {
		var count = parseInt(req.params.count);
		var r = [];
		for (var i = 0; i < count; ++i) {
			r.push({
				meta : "foo",
				id   : i,
				queue: req.params.queue
			});
		}
		res.send({
			results: r
		})
	} else {
		res.send({
			results: {
				meta : "foo",
				id   : "deadbeef",
				queue: req.params.queue
			}
		})
	}
});

/*
 * This is the endpoint for updating a heartbeat
 */
app.post('/heartbeat/:queue', function(req, res) {
	var params = req.body;
	var r = [];
	for (var i in params.data) {
		r.push(300);
	}
	res.send({
		results: r
	});
});

/*
 * This is the endpoint for returning a piece of work
 */
app.post('/complete/:queue', function(req, res) {
	var params = req.body;
	var r = [];
	for (var i in params.data) {
		r.push(params.data[i].id);
	}
	res.send({
		results: r
	});
});

/*
 * This endpoint allows you to add a work item
 */
app.post('/add/:queue', function(req, res) {
	var q = new queue.Queue(req.params.queue);
	q.push(req.body.data, function(err, results) {
		res.send({
			results: results
		});
	});
});

/*
 * This endpoint lists all known queues
 */
app.get('/queues', function(req, res) {
	queue.list(function(r) {
		res.send({
			results: r
		});
	});
});

/*
 * This endpoint is the UI
 */
app.get('/ui', function(req, res) {
	queue.list(function(r) {
		res.render('index.haml', {
			items: r
		});
	});
});

app.listen(3000);