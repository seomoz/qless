#! /usr/bin/env python

import qless
import unittest

class TestQless(unittest.TestCase):
    def setUp(self):
        self.q = qless.Queue('testing')
    
    def test_push_len(self):
        self.assertEqual(len(self.q), 0, 'Start with an empty queue')
        self.q.put(qless.Job(0, {'hello': 'how are you'}))
        self.assertEqual(len(self.q), 1, 'Inserting should increase the size of the queue')

if __name__ == '__main__':
    unittest.main()