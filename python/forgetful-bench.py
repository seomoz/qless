#! /usr/bin/env python

import argparse

# First off, read the arguments
parser = argparse.ArgumentParser(description='Run forgetful workers on contrived jobs.')

parser.add_argument('--forgetfulness', dest='forgetfulness', default=0.1, type=float,
    help='What portion of jobs should be randomly dropped by workers')
parser.add_argument('--jobs', dest='numJobs', default=1000, type=int,
    help='How many jobs to schedule for the test')
parser.add_argument('--workers', dest='numWorkers', default=10, type=int,
    help='How many workers should do the work')
parser.add_argument('--quiet', dest='verbose', default=True, action='store_false',
    help='Reduce all the output')

args = parser.parse_args()

import time
import qless
import random
import logging
import threading

logger = logging.getLogger('qless-bench')
formatter = logging.Formatter('[%(asctime)s] %(threadName)s => %(message)s')
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)
if args.verbose:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.WARN)

class ForgetfulWorker(threading.Thread):
    def __init__(self, *args, **kwargs):
        threading.Thread.__init__(self, *args, **kwargs)
        self.q = qless.Queue('testing')
        self.q._hb = 1
        self.q.worker += '-' + self.getName() 
    
    def run(self):
        while len(self.q):
            job = self.q.pop()
            if not job:
                # Sleep a little bit
                time.sleep(0.1)
                logger.debug('No jobs available. Sleeping.')
                continue
            # Randomly drop a job?
            if random.random() < args.forgetfulness:
                logger.debug('Randomly dropping job!')
                continue
            else:
                logger.debug('Completing job!')
                self.q.complete(job)

# Make sure that the redis instance is empty first
if len(qless.r.keys('*')):
    print 'Must begin on an empty Redis instance'
    exit(1)

# This is how much CPU Redis had used /before/
cpuBefore = qless.r.info()['used_cpu_user'] + qless.r.info()['used_cpu_sys']
# This is how long it took to add the jobs
putTime = -time.time()
# Alright, let's make a bunch of jobs
jids = [qless.Job.put('testing', {'test': 'benchmark', 'count': c}) for c in range(args.numJobs)]
putTime += time.time()

# This is how long it took to run the workers
workTime = -time.time()
# And now let's make some workers to deal with 'em!
workers = [ForgetfulWorker() for i in range(args.numWorkers)]
for worker in workers:
    worker.start()

for worker in workers:
    worker.join()

workTime += time.time()

import unittest
class TestQlessBench(unittest.TestCase):
    def test_stats(self):
        # In this test, we want to make sure that the stats we get out of 
        # the system seem in accordance with what we'd expect. Ensure:
        #   1) Completed all of the jobs
        #   2) Non-zero mean, non-zero std deviation
        #   3) Wait stats for all jobs
        #   4) Non-zero 
        stats = qless.Stats.get('testing', time.time())
        self.assertEqual(stats['run']['count'], args.numJobs)
        self.assertTrue( stats['run']['mean' ], 0)
        self.assertTrue( stats['run']['std'  ], 0)
        # We may want to ensure that it's within certain expectation
        # values based on the failure rate
        self.assertTrue(stats['wait']['count'] > args.numJobs)
        self.assertTrue(stats['wait']['mean' ] > 0)
        self.assertTrue(stats['wait']['std'  ] > 0)

# This needs some work
#unittest.main(exit=False)

def histo(l):
    count = sum(l)
    l = list(o for o in l if o)
    for i in range(len(l)):
        print '\t\t%2i, %10.9f, %i' % (i, float(l[i]) / count, l[i])

# Now we'll print out some interesting stats
stats = qless.Stats.get('testing', time.time())
print 'Wait:'
print '\tCount: %i'  % stats['wait']['count']
print '\tMean : %fs' % stats['wait']['mean']
print '\tSDev : %f'  % stats['wait']['std']
print '\tHist :'
histo(stats['wait']['histogram'])

print 'Run:'
print '\tCount: %i'  % stats['run']['count']
print '\tMean : %fs' % stats['run']['mean']
print '\tSDev : %f'  % stats['run']['std']
histo(stats['run']['histogram'])

print '=' * 50
print 'Put jobs : %fs' % putTime
print 'Do jobs  : %fs' % workTime
info = qless.r.info()
print 'Redis Mem: %s'  % info['used_memory_human']
print 'Redis Lua: %s'  % info['used_memory_lua']
print 'Redis CPU: %fs' % (info['used_cpu_user'] + info['used_cpu_sys'] - cpuBefore)

# Flush the database when we're done
print 'Flushing'
qless.r.flushdb()