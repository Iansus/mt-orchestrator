#!/usr/bin/python

import logging
import orchestrator
import time
import sys

from mtorch.task import *
from mtorch.orchestrator import Orchestrator

logging.basicConfig(format = '\r[%(asctime)s] %(name)-15s %(levelname)-9s %(message)s')
timerno = 1

class Timer(Task):

    def __init__(self, timer, **kwargs):

        global timerno
        kwargs['name'] = 'timer%d' % timer
        Task.__init__(self, **kwargs)
        self.timer = timer
        timerno += 1

    def run(self):
        self.log(logging.DEBUG, 'Timer started')
        time.sleep(self.timer)

        self.log(logging.DEBUG, 'Timer ended')
        self.setExecutionState(EXEC_SUCCEEDED)
        self.setCompleted()


o = Orchestrator(threads=4, verbosity=logging.DEBUG)

t = None
for i in range(1, 10, 2):
    t = Timer(i)
    o.addTask(t)

o.start()

for i in range(2, 10, 2):
    t = Timer(i)
    o.addTask(t)
