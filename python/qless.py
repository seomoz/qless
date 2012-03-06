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
    _stats = lua('stats')
    
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
    _get       = lua('get')
    _put       = lua('put')
    _heartbeat = lua('heartbeat')
    
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
        return self._put([queue], [
            uuid.uuid1().hex,
            json.dumps(data),
            priority or 0,
            json.dumps(tags or []),
            delay or 0
        ])
    
    def __init__(self, id, data, priority, tags, expires, state, queue, history):
        self.id       = id
        self.data     = data or {}
        self.priority = priority
        self.tags     = tags
        self.expires  = expires
        self.state    = state
        self.queue    = queue
        self.history  = history or []
    
    def __getitem__(self, key):
        return self.data.get(key)
    
    def __setitem__(self, key, value):
        self.data[key] = value

    # Complete(0, id, [data, [queue, [delay]]])
    # -----------------------------------------------
    # Complete a job and optionally put it in another queue, either scheduled or to
    # be considered waiting immediately.    
    def complete(self, queue=None, delay=None):
        return self._complete([], [self.id, queue or '', delay or 0])
    
    # Fail(0, id, type, message, now)
    # -------------------------------
    # Mark the particular job as failed, with the provided type, and a more specific
    # message. By `type`, we mean some phrase that might be one of several categorical
    # modes of failure. The `message` is something more job-specific, like perhaps
    # a traceback.
    def fail(self, id, t, message):
        return self._fail([], [id, t, message, time.time()])
    
    # Heartbeat(0, id, worker, expiration, [data])
    # -------------------------------------------
    # Renew the heartbeat, if possible, and optionally update the job's user data.
    def heartbeat(self, heartbeat):
        return self._heartbeat([], [id, getWorkerName(), time.time() + heartbeat, json.dumps(self.data)])
    
    def delete(self):
        '''Deletes the job from the database'''
        r.delete('ql:j:' + self.id)

class Queue(object):
    # This is the worker identification
    worker    = getWorkerName()
    # This is the heartbeat interval, in seconds
    heartbeat = 60
    
    def __init__(self, name, *args, **kwargs):
        self.name     = name
        self._pop     = lua('pop')
        self._peek    = lua('peek')
    
    def push(self, job, heartbeat):
        job.put(self.name, heartbeat)
    
    # Pop(1, queue, worker, count, now, expiration)
    # ---------------------------------------------
    # Passing in the queue from which to pull items, the current time, when the locks
    # for these returned items should expire, and the number of items to be popped
    # off.    
    def pop(self, count=1):
        return [json.loads(j) for j in self._pop([self.name], [self.worker, count, time.time()])]
    
    # Peek(1, queue, count, now)
    # --------------------------
    # Similar to the `Pop` command, except that it merely peeks at the next items
    # in the queue.
    def peek(self, count=1):
        return self._peek([self.name], [count, time.time()])
    
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