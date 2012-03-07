#! /usr/bin/env python

import time
import uuid
import redis
import logging
import simplejson as json

logger = logging.getLogger('qless')

r = redis.Redis()
base = '/Users/dan/SEOmoz/qless/lua/'

# This is the identifier we'll use as this worker's moniker
def getWorkerName():
    import os
    import socket
    return socket.gethostname() + '-' + str(os.getpid())

class lua(object):
    def __init__(self, name):
        self.name = name
        self.reload()

    def reload(self):
        with file(base + self.name + '.lua') as f:
            self.sha = r.execute_command('script', 'load', f.read())
            logger.debug('Loaded script ' + self.sha)        
    
    def __call__(self, keys, args):
        try:
            return r.execute_command('evalsha', self.sha, len(keys), *(keys + args))
        except Exception as e:
            self.reload()
            return r.execute_command('evalsha', self.sha, len(keys), *(keys + args))

class Stats(object):
    _stats  = lua('stats')
    _failed = lua('failed')
    
    # Stats(0, queue, date)
    # ---------------------
    # Return the current statistics for a given queue on a given date. The results 
    # are returned are a JSON blob:
    # 
    #     {
    #         'total'    : ...,
    #         'mean'     : ...,
    #         'variance' : ...,
    #         'histogram': [
    #             ...
    #         ]
    #     }
    # 
    # The histogram's data points are at the second resolution for the first minute,
    # the minute resolution for the first hour, the 15-minute resolution for the first
    # day, the hour resolution for the first 3 days, and then at the day resolution
    # from there on out. The `histogram` key is a list of those values.
    @staticmethod
    def get(queue, date):
        return json.loads(Stats._stats([], [queue, date]))
    
    # Failed(0, [type, [start, [limit]]])
    # -----------------------------------
    # If no type is provided, this returns a JSON blob of the counts of the various
    # types of failures known. If a type is provided, it will report up to `limit`
    # from `start` of the jobs affected by that issue. __Returns__ a JSON blob.
    @staticmethod
    def failed(t=None, start=0, limit=25):
        if not t:
            return json.loads(Stats._failed([], []))
        else:
            return json.loads(Stats._failed([], [t, start, limit]))

class Config(object):
    _get = lua('getconfig')
    _set = lua('setconfig')
    
    # GetConfig(0, [option])
    # ----------------------
    # Get the current configuration value for that option, or if option is omitted,
    # then get all the configuration values.
    @staticmethod
    def get(option=None):
        if option:
            return Config._get([], [option])
        else:
            # This is taken from redis-py:redis/client.py
            from itertools import izip
            it = iter(Config._get([], []))
            return dict(izip(it, it))
        
    # SetConfig(0, option, [value])
    # -----------------------------
    # Set the configuration value for the provided option. If `value` is omitted,
    # then it will remove that configuration option.
    @staticmethod
    def set(option, value=None):
        if value:
            return Config._set([], [option, value])
        else:
            return Config._set([], [option])

class Job(object):
    _get = lua('get')
    _put = lua('put')
    
    # Get(0, id)
    # ----------
    # Get the data associated with a job
    @classmethod
    def get(cls, id):
        return Job(**json.loads(Job._get([], [id])))
    
    # Put(1, queue, id, data, now, [priority, [tags, [delay]]])
    # ---------------------------------------------------------------    
    # Either create a new job in the provided queue with the provided attributes,
    # or move that job into that queue. If the job is being serviced by a worker,
    # subsequent attempts by that worker to either `heartbeat` or `complete` the
    # job should fail and return `false`.
    # 
    # The `priority` argument should be negative to be run sooner rather than 
    # later, and positive if it's less important. The `tags` argument should be
    # a JSON array of the tags associated with the instance and the `valid after`
    # argument should be in how many seconds the instance should be considered 
    # actionable.
    @staticmethod
    def put(queue, data, priority=None, tags=None, delay=None):
        return Job._put([queue], [
            uuid.uuid1().hex,
            json.dumps(data),
            time.time(),
            priority or 0,
            json.dumps(tags or []),
            delay or 0
        ])
    
    def __init__(self, id, data, priority, tags, worker, expires, state, queue, history=[]):
        self.id       = id
        self.data     = data or {}
        self.priority = priority
        self.tags     = tags or []
        self.worker   = worker
        self.expires  = expires
        self.state    = state
        self.queue    = queue
        self.history  = history or []
    
    def __getitem__(self, key):
        return self.data.get(key)
    
    def __setitem__(self, key, value):
        self.data[key] = value
    
    def __str__(self):
        import pprint
        s  = 'qless:Job : %s\n' % self.id
        s += '\tpriority: %i\n' % self.priority
        s += '\ttags: %s\n' % ', '.join(self.tags)
        s += '\tworker: %s\n' % self.worker
        s += '\texpires: %i\n' % self.expires
        s += '\tstate: %s\n' % self.state
        s += '\tqueue: %s\n' % self.queue
        s += '\thistory:\n'
        for h in self.history:
            s += '\t\t%s (%s)\n' % (h['queue'], h['worker'])
            s += '\t\tput: %i\n' % h['put']
            if h['popped']:
                s += '\t\tpopped: %i\n' % h['popped']
            if h['completed']:
                s += '\t\tcompleted: %i\n' % h['completed']
        s += '\tdata: %s' % pprint.pformat(self.data)
        return s
    
    def __repr__(self):
        return '<qless:Job %s>' % self.id
    
    # Fail(0, id, type, message, now)
    # -------------------------------
    # Mark the particular job as failed, with the provided type, and a more specific
    # message. By `type`, we mean some phrase that might be one of several categorical
    # modes of failure. The `message` is something more job-specific, like perhaps
    # a traceback.
    def fail(self, id, t, message):
        return self._fail([], [id, t, message, time.time()])
    
    # Put(1, queue, id, data, now, [priority, [tags, [delay]]])
    # ---------------------------------------------------------------    
    # Either create a new job in the provided queue with the provided attributes,
    # or move that job into that queue. If the job is being serviced by a worker,
    # subsequent attempts by that worker to either `heartbeat` or `complete` the
    # job should fail and return `false`.
    # 
    # The `priority` argument should be negative to be run sooner rather than 
    # later, and positive if it's less important. The `tags` argument should be
    # a JSON array of the tags associated with the instance and the `valid after`
    # argument should be in how many seconds the instance should be considered 
    # actionable.
    def move(self, queue):
        return Job._put([queue], [
            self.id,
            json.dumps(self.data),
            time.time()
        ])
    
    def delete(self):
        '''Deletes the job from the database'''
        r.delete('ql:j:' + self.id)

class Queue(object):
    # This is the worker identification
    worker = getWorkerName()
    # This is the heartbeat interval, in seconds
    _hb    = 60
    
    # Our lua scripts
    _pop       = lua('pop')
    _fail      = lua('fail')
    _peek      = lua('peek')
    _complete  = lua('complete')
    _heartbeat = lua('heartbeat')
    
    def __init__(self, name, *args, **kwargs):
        self.name     = name
    
    # Put(1, queue, id, data, now, [priority, [tags, [delay]]])
    # ---------------------------------------------------------------    
    # Either create a new job in the provided queue with the provided attributes,
    # or move that job into that queue. If the job is being serviced by a worker,
    # subsequent attempts by that worker to either `heartbeat` or `complete` the
    # job should fail and return `false`.
    # 
    # The `priority` argument should be negative to be run sooner rather than 
    # later, and positive if it's less important. The `tags` argument should be
    # a JSON array of the tags associated with the instance and the `valid after`
    # argument should be in how many seconds the instance should be considered 
    # actionable.
    def put(self, *args, **kwargs):
        return Job.put(self.name, *args, **kwargs)
    
    # Pop(1, queue, worker, count, now, expiration)
    # ---------------------------------------------
    # Passing in the queue from which to pull items, the current time, when the locks
    # for these returned items should expire, and the number of items to be popped
    # off.    
    def pop(self, count=None):
        results = [Job(**json.loads(j)) for j in self._pop([self.name], [self.worker, count or 1, time.time(), time.time() + self._hb])]
        if count == None:
            return (len(results) and results[0]) or None
        return results
    
    # Peek(1, queue, count, now)
    # --------------------------
    # Similar to the `Pop` command, except that it merely peeks at the next items
    # in the queue.
    def peek(self, count=None):
        results = [Job(**json.loads(r)) for r in self._peek([self.name], [count or 1, time.time()])]
        if count == None:
            return (len(results) and results[0]) or None
        return results
    
    # Fail(0, id, worker, type, message, now, [data])
    # -----------------------------------------------
    # Mark the particular job as failed, with the provided type, and a more specific
    # message. By `type`, we mean some phrase that might be one of several categorical
    # modes of failure. The `message` is something more job-specific, like perhaps
    # a traceback.
    # 
    # This method should __not__ be used to note that a job has been dropped or has 
    # failed in a transient way. This method __should__ be used to note that a job has
    # something really wrong with it that must be remedied.
    # 
    # The motivation behind the `type` is so that similar errors can be grouped together.
    # Optionally, updated data can be provided for the job. A job in any state can be
    # marked as failed. If it has been given to a worker as a job, then its subsequent
    # requests to heartbeat or complete that job will fail. Failed jobs are kept until
    # they are canceled or completed. __Returns__ the id of the failed job if successful,
    # or `False` on failure.
    def fail(self, job, t, message):
        return self._fail([], [job.id, self.worker, t, message, time.time(), json.dumps(job.data)]) or False
    
    # Heartbeat(0, id, worker, expiration, [data])
    # -------------------------------------------
    # Renew the heartbeat, if possible, and optionally update the job's user data.
    def heartbeat(self, job):
        return float(self._heartbeat([], [job.id, self.worker, time.time() + self._hb, json.dumps(job.data)]) or 0)
    
    # Complete(0, id, worker, queue, now, [data, [next, [delay]]])
    # -----------------------------------------------
    # Complete a job and optionally put it in another queue, either scheduled or to
    # be considered waiting immediately.    
    def complete(self, job, next=None, delay=None):
        if next:
            return self._complete([], [job.id, self.worker, self.name,
                time.time(), json.dumps(job.data), next, delay or 0]) or False
        else:
            return self._complete([], [job.id, self.worker, self.name,
                time.time(), json.dumps(job.data)]) or False
    
    def delete(self):
        with r.pipeline() as p:
            o = p.delete('ql:q:' + self.name + '-work')
            o = p.delete('ql:q:' + self.name + '-locks')
            o = p.delete('ql:q:' + self.name + '-scheduled')
            return bool(p.execute())
    
    def __len__(self):
        with r.pipeline() as p:
            o = p.zcard('ql:q:' + self.name + '-locks')
            o = p.zcard('ql:q:' + self.name + '-work')
            o = p.zcard('ql:q:' + self.name + '-scheduled')
            return sum(p.execute())    