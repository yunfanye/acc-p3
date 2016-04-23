#!/usr/bin/env python

#trace file format : arrivalTime startTime finishTime k prio (comma delim)
#trace file example: 796.595492430708,796.595492430708,856.595492430708,11,0
#assume that (finish-start ~ fast_duration)

import json
import os, errno
import os.path
import sys
import getopt
import time
import string
import shutil
import time
import random

DRYRUN = 0
#assume CWD = exp/
HADOOP_HOME = os.getenv("HADOOP_HOME")
assert(HADOOP_HOME)
print HADOOP_HOME

def genmpicmd(appname, mpicmd, k, pol, timeoutMs):
    timeoutMs = 1200*1000 #20min*60s/min * 1000 (ms)
    cmd = os.path.join(HADOOP_HOME, "bin/hadoop") \
        + " jar %s/hadoop-yarn-applications-mpirunner-2.2.0.jar org.apache.hadoop.yarn.applications.mpirunner.Client" %HADOOP_HOME\
        + " --jar %s/hadoop-yarn-applications-mpirunner-2.2.0.jar " %HADOOP_HOME \
        + " --shell_cmd_priority 5 --appname %s" %appname        \
        + " --master_memory 32 "        \
        + " --shell_command %s/%s " %(HADOOP_HOME, mpicmd)  \
        + " --num_containers %d " %k    \
        + " --container_memory 7168 " \
        + " --timeout %d" %timeoutMs \
        + " --policy %s" %pol
    return cmd

def gengpucmd(appname, gpucmd, k, pol, timeoutMs):
    timeoutMs = 1200*1000 #20min*60s/min * 1000 (ms)
    cmd = os.path.join(HADOOP_HOME, "bin/hadoop" ) \
        + " jar %s/hadoop-yarn-applications-gpu-2.2.0.jar org.apache.hadoop.yarn.applications.gpu.Client " %HADOOP_HOME\
        + " --jar %s/hadoop-yarn-applications-gpu-2.2.0.jar " %HADOOP_HOME       \
        + " --shell_cmd_priority 5 --appname %s " %appname \
        + " --master_memory 32 "                           \
        + " --shell_command 'OMP_NUM_THREADS=%%v\ %s/%s' " %(HADOOP_HOME, gpucmd) \
        + " --num_containers %d" %k  \
        + " --numCoresBig 4 "      \
        + " --numCoresSmall 2 "     \
        + " --bigNodes 'r1h0,r1h1,r1h2,r1h3'"   \
        + " --container_memory 7168" \
        + " --timeout %d" %timeoutMs \
        + " --policy %s" %pol
    return cmd

def genhadoopcmd(jobtstr,appname,cmdstr,k,dur, pol, timeoutMs):
    #switch on jobt
    if jobtstr == "MPI":
        return genmpicmd(appname,cmdstr,k, pol, timeoutMs)
    if jobtstr == "GPU":
        return gengpucmd(appname,cmdstr,k, pol, timeoutMs)

    return None

if __name__ == "__main__":

    if len(sys.argv) < 3:
        print "USAGE: " + sys.argv[0] + " -c configfile -t tracef "
        sys.exit(1)

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'c:t:')
    except:
        print 'error parsing args'
        sys.exit(1)

    for o,a in opts:
        if o in ('-c'):
            config = a
        elif o in ('-t'):
            tracef = a
        else:
            print 'unrecognized option', o
            print "USAGE: " + sys.argv[0] + " -c configfile [-t tracef ]"
            sys.exit(1)

    fpjson = file(config, 'r')
    base_cfg = json.load(fpjson)
    fpjson.close()

    #determine the job type
    jobtstr = None
    if tracef.find('traceMPI') >=0 :
        jobtstr = "MPI"
    elif tracef.find('traceGPU') >=0:
        jobtstr = "GPU"
    else:
        jobtstr = "NOTIMPLEMENTED"
        sys.exit(0)

    jobt = base_cfg["traces"][jobtstr]["jobtype"]
    jobmap = {} # resolves fastdur to job and slowdur
    sched_pol = base_cfg["simtype"]

    #MPI
    if "durationKList" in base_cfg["traces"][jobtstr].keys():
        # build the map using durationKList (e.g., for MPI jobs)
        durationKList = base_cfg["traces"][jobtstr]["durationKList"]
        #just insert each struct into the map
        for j in durationKList:
            jobmap[(j["duration"], j["k"])] = (j["job"], j["slowduration"])
    #OpenMP (aka GPU)
    elif "durationList" in base_cfg["traces"][jobtstr].keys():
        #build the map using durationList (e.g., for GPU jobs)
        durationList = base_cfg["traces"][jobtstr]["durationList"]
        for j in durationList:
            jobmap[j["duration"]] = (j["job"], j["slowduration"])

    else:
        # don't really have the job or slowduration for these types yet
        # leave jobmap empty
        pass

    print "JOB MAP"
    print jobmap

    # now replay the trace
    fp = file(tracef, 'r')
    basetime = time.time()
    for line in fp.readlines():
        (arrt, startt, finisht, k, prio) = line.strip().split(',')
        #convert strings to numerics...
        k = int(k)
        prio = int(prio)
        arrt = float(arrt)
        startt = float(startt)
        finisht = float(finisht)
        fastdur = finisht - startt
        fastdur = round(fastdur)    #round to nearest second
        #map lookup
        if jobtstr == "MPI":
            lookupkey = (fastdur, k)
        else:
            lookupkey = fastdur

        if lookupkey not in jobmap.keys():
            print "WARN JOB type:(%s, %s) NOT in the job map for traceline: %s" %(jobtstr,base_cfg["traces"][jobtstr],line)
            #assign default job name and meanDuration*slowdown
            slowdur = fastdur
            cmdstr = "ls"
        else:
            print "INFO: JOB type:(%s, %s) found in the job map" %(jobtstr, base_cfg["traces"][jobtstr])
            (cmdstr , slowdur) = jobmap[lookupkey]
        #construct AM name with format : jobt-k-prio-duration-slowduration
        jobname = "%s-%s-%s-%s-%s" %(jobt,k,prio,fastdur,slowdur)

        #calculate timeout
        if (jobt == 0) or (jobt == 1) or (jobt == 2) or (jobt == 5):
            timeout = fastdur + (2.0 * slowdur)
        timeoutMs = int(timeout * 1000.0)

        #generate command
        hadoopcmd = genhadoopcmd(jobtstr,jobname,cmdstr,k,fastdur, base_cfg["simtype"], timeoutMs)

        curtime = time.time() #get the current time and sleep until arrival time
        # job's supposed to arrive at arrt+basetime
        deltat = arrt+basetime - curtime
        if deltat > 0 :
            print "Sleeping for " + str(deltat) + " seconds"
            time.sleep(deltat)

        print hadoopcmd
        if not DRYRUN:
            os.system(hadoopcmd + " &") #launch the job in the background

    fp.close()
