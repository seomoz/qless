#! /usr/bin/env python

import uuid
import redis
import simplejson as json

r = redis.Redis()
base = '/Users/dlecocq/projects/qless/lua/'

class lua(object):
    def __init__(self, name):
        self.name = name
        # Find the file
        with file(base + name + '.lua') as f:
            self.sha = r.execute_command('script', 'load', f.read())
    
    def __call__(self, *args):
        try:
            return r.execute_command('evalsha', self.sha, 1, args[0], *args[1:])
        except Exception as e:
            with file(base + self.name + '.lua') as f:
                self.sha = r.execute_command('script', 'load', f.read())
            return r.execute_command('evalsha', self.sha, 1, args[0], *args[1:])

class Job(object):
    def __init__(self, priority, data, tags=[], valid_after=0):
        self.id          = uuid.uuid1().hex
        self.priority    = str(priority)
        self.data        = json.dumps(data)
        self.tags        = json.dumps(tags)
        self.valid_after = valid_after

class Queue(object):
    def __init__(self, name):
        self.name     = name
        self._put     = lua('put')
        #self._destroy = lua('destroy')
    
    def put(self, job):
        self._put(*[self.name, job.id, job.priority, job.data, job.tags, job.valid_after])
    
    def destroy(self):
        self._destroy(*[self.name])
    
    def __len__(self):
        with r.pipeline() as p:
            o = p.zcard('ql:q:' + self.name + '-locks')
            o = p.zcard('ql:q:' + self.name + '-work')
            o = p.zcard('ql:q:' + self.name + '-scheduled')
            return sum(p.execute())    