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

class Config(object):
    _get = lua('getconfig')
    _set = lua('setconfig')
    
    @staticmethod
    def get(option=None):
        if option:
            return Config._get([], [option])
        else:
            # This is taken from redis-py:redis/client.py
            from itertools import izip
            it = iter(Config._get([], []))
            return dict(izip(it, it))
    
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
    
    @classmethod
    def get(cls, id):
        return json.loads(Job._get([], [id]))
    
    def __init__(self, data, priority=0, tags=[], valid_after=None):
        self.id          = uuid.uuid1().hex
        self.priority    = priority
        self.data        = json.dumps(data)
        self.tags        = json.dumps(tags)
        self.valid_after = valid_after or time.time()
    
    def save(self):
        pass
    
    def heartbeat(self):
        '''Perform the heartbeat with any updated data'''
        pass
    
    def delete(self):
        '''Deletes the job from the database'''
        r.delete('ql:j:' + self.id)
    
    def put(self, qname, heartbeat):
        '''Put this item in such-and-such queue'''
        return self._put([qname], [self.id, self.data, time.time(), self.priority, self.tags, self.valid_after, heartbeat])

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
    
    def pop(self, count=1):
        return [json.loads(j) for j in self._pop([self.name], [self.worker, count, time.time()])]
    
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