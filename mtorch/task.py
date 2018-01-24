#!/usr/bin/python

import threading
import logging
import uuid
import sys
import time

# Logging config
logging.basicConfig(format = '\r[%(asctime)s] %(name)-15s %(levelname)-9s %(message)s')

# Execution states
EXEC_WAITING = 2
EXEC_SUCCEEDED = 1
EXEC_ACTIVE = 0
EXEC_ABORTED = -1


# Task interface - abstract class
class Task(threading.Thread):

    def __init__(self, **kwargs):
        threading.Thread.__init__(self)

        name = kwargs.get('name', None)
        self.__name = str(uuid.uuid4())[:8] if name is None else name
        self.__returnValue = None
        self.__dependencies = kwargs.get('deps', [])
        self.__lock = None
        self.__logger = None
        self.__success = False
        self.__execState = EXEC_WAITING


    # Logger
    def log(self, level, message):
        if self.__logger is None:
            return

        self.__logger.log(level, message)


    # Getters / Setters
    def setLock(self, lock):
        self.__lock = lock


    def setParentLogger(self, parentLogger):
        self.__logger = parentLogger.getChild('%s' % ( self.__name))


    def isCompleted(self):
        return self.__execState in (EXEC_ABORTED, EXEC_SUCCEEDED)


    def getExecutionState(self):
        return self.__execState


    def setCompleted(self):
        self.__completed = True


    def setExecutionState(self, state):
        self.__execState = state


    def getDependencies(self):
        return self.__dependencies


    def getReturnValue(self):
        return self.__returnValue


    def setReturnValue(self, returnValue):
        self.__returnValue = returnValue


    def getName(self):
        return self.__name

    # Thread safe operations
    def __safeWrite(self, fd = sys.stdout, data = ''):
        if self.__lock is None:
            raise Exception('Lock not set')

        self.__lock.acquire()
        fd.write(data)
        fd.flush()
        self.__lock.release()


    def __safeRead(self, fd = sys.stdin, size=1024):
        if self.__lock is None:
            raise Exception('Lock not set')

        self.__lock.acquire()
        data = fd.read(size)
        self.__lock.release()

        return data


    # Abstract method
    def run(self):
        raise NotImplementedError
