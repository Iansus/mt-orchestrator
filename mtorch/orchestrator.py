#!/usr/bin/python

import threading
import logging
import uuid
import sys
import time

import task

# Logging config
logging.basicConfig(format = '\r[%(asctime)s] %(name)-15s %(levelname)-9s %(message)s')

# Orchestrator class
class Orchestrator(threading.Thread):

    def __init__(self, name='orch', threads=1, verbosity=logging.INFO):

        threading.Thread.__init__(self)

        self.__name = name
        self.__threadCount = threads
        self.__logger = logging.getLogger(name)
        self.__logger.setLevel(verbosity)

        self.__waitingQueue = []
        self.__activePool = []
        self.__inactiveList = []

        self.__lock = threading.Lock()
        self.__completed = False


    # Logger
    def __log(self, level, message):
        self.__logger.log(level, message)


    # Tasks handling
    def addTask(self, task):
        task.setParentLogger(self.__logger)
        task.setLock(self.__lock)

        self.__waitingQueue.append((task, None))
        self.__log(logging.DEBUG, 'Task %s added' % task.getName())


    def getProgress(self):
        return {
            'w': len(self.__waitingQueue),
            'a': len(self.__activePool),
            'i': len(self.__inactiveList)
        }


    def run(self):

        self.__log(logging.INFO, 'Orchestrator %s started' % self.__name)
        while len(self.__waitingQueue)>0 or len(self.__activePool)>0:

            for activeTask in self.__activePool:
                if activeTask.isCompleted():
                    self.__activePool.remove(activeTask)
                    self.__inactiveList.append(activeTask)

            while len(self.__activePool) < self.__threadCount:
                if len(self.__waitingQueue)==0:
                    break

                newTask, waitTime = self.__waitingQueue.pop(0)
                if waitTime is not None:
                    time.sleep(waitTime)

                newTaskState = task.EXEC_ACTIVE
                for dependency in newTask.getDependencies():

                    self.__log(logging.DEBUG, 'Task %s has dependency on %s' % (newTask.getName(), dependency.getName()))
                    if not dependency.isCompleted():
                        self.__log(logging.DEBUG, 'Cannot add task %s because task %s has not yet finished' % (newTask.getName(), dependency.getName()))
                        newTaskState = task.EXEC_WAITING
                        break

                    elif dependency.getExecutionState() == task.EXEC_ABORTED:
                        newTaskState = task.EXEC_ABORTED
                        break

                newTask.setExecutionState(newTaskState)
                if newTaskState == task.EXEC_WAITING:
                    waitTime = 0.1 if waitTime is None else 2*waitTime
                    self.__waitingQueue.append((newTask, waitTime))

                elif newTaskState == task.EXEC_ACTIVE:
                    self.__activePool.append(newTask)
                    newTask.daemon = True
                    newTask.start()

                elif newTask == task.EXEC_ABORTED:
                    newTask.setCompleted()
                    self.__inactiveList.append(newTask)


        self.__log(logging.INFO, 'Orchestrator %s ended' % self.__name)
        self.__completed = True


    def getInactiveList(self):
        return self.__inactiveList


    def getActivePool(self):
        return self.__activePool


    def getWaitingQueue(self):
        return self.__waitingQueue


    def hasCompleted(self):
        return self.__completed
