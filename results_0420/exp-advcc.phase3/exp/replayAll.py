#!/usr/bin/env python
import os
import sys
import os.path
import getopt

DRYRUN=0
MINARGS=5

def replayAll(replaybindir, configf, tracefiles):
    for tracef in tracefiles:
        #start all replayers in the background
        if not DRYRUN:
            os.system('%s/replayTrace.py -c %s -t %s  &' %(replaybindir,configf,tracef))

if __name__ == "__main__":

    if len(sys.argv) < MINARGS:
        print "USAGE: %s -c configfile -t trace1[,trace2,trace3]" %sys.argv[0]
        sys.exit(1)

    try:                                                                 
        opts, args = getopt.getopt(sys.argv[1:], 'c:t:')
    except:                                                              
        print 'error parsing args'                                       
        raise
        sys.exit(1)                                                      
                                                                         
    for o,a in opts:                                                     
        if o in ('-c'):                                                  
            configf = a                                                   
        elif o in ('-t'):                                                
            tracefcsv = a                                                   
        else:                                                            
            print 'unrecognized option', o                               
            print "USAGE: " + sys.argv[0] + " -c configfile -t trace1[,trace2,trace3]"
            sys.exit(1)                                                  

    tracefiles = tracefcsv.strip().split(',')
    print "replaying specified tracefiles : ", tracefiles

    #try to guess script directory: assume the same
    (replaybindir, replaybinfile) = os.path.split(sys.argv[0])

    replayAll(replaybindir, configf, tracefiles)
