#!/usr/bin/env python

import os
import sys

MIN = 60
maxT = 20*MIN
epsilon = 0.01
resultsFilename = "results.txt"

if len(sys.argv) > 1:
    resultsFilename = sys.argv[1]
    print "analyzing %s" %resultsFilename

# Get initial time
initialTime = float('inf')
with open(resultsFilename, "r") as fp:
    for line in fp:
        data = line.rstrip("\r\n").split(",")
        initialTime = min(initialTime, float(data[1]))

with open(resultsFilename, "r") as fp:
    totalT = 0
    totalU = 0
    totalN = 0
    for line in fp:
        data = line.rstrip("\r\n").split(",")
        name = data[0]
        #timestamps are in ms --> convert them to to seconds
        submitTime = ((float(data[1]) - initialTime) / 1000.0)
        startTime = ((float(data[2]) - initialTime) / 1000.0)
        runningTime = ((float(data[3]) - initialTime) / 1000.0)
        finishTime = ((float(data[4]) - initialTime) / 1000.0)
        status = data[5]
        statusletter = '?' #unknown
        if status.lower().startswith('kill'):
            statusletter = 'K'
        elif status.lower().startswith('finish'):
            statusletter = 'F'
        AMnodeName = data[6].split("/")[0]
        nodeNames = data[7].split("|")
        params = name.split("-")
        jobType = int(params[0])
        k = int(params[1])
        priority = int(params[2])
        estDuration = float(params[3])
        estSlowDuration = float(params[4])

        qdelay = runningTime - submitTime
        realduration = finishTime - runningTime
        totalcompletiontime = finishTime - submitTime

        assert(abs(totalcompletiontime - qdelay -realduration) < epsilon)

        #print "%s\t: T: %7.2f\t Q: %.2f\t R: %.2f" %(name, totalcompletiontime, qdelay, realduration)
        #assume U(T) = maxT - T
        util = maxT - totalcompletiontime
        print "%s\t: %s\tT: %7.2f\tU:%8.2f" %(name, statusletter, totalcompletiontime, util)
        totalT += totalcompletiontime
        totalU += util
        totalN += 1
    print "mean completion time (E[T]) = %s" % (totalT/totalN)
    print "total utility = %s" %(totalU)


