15-719 S16: Project 2 Phase 3
=====

Running Experiments
-------------------
### multi-trace experiment (an example)
* Set the simtype config parameter to one of "soft", "hard", or "none"
* Restart your YARN RM
* Start your schedpolserver:
` src/schedpolserver -c config/config-timex1-c2x4-g4-h6-rho0.70`
* Run the experiment:
`exp/replayAll.py -c config/config-timex1-c2x4-g4-h6-rho0.70 -t traces/traceMPI-c2x4-rho0.80,traces/traceGPU-c2x4-rho0.80`
* copy/move the results.txt file to a corresponding results file:
`mv $HADOOP_HOME/results.txt results/traceCombined-c2x4-rho0.80.result.<simtype>`
* Analyze results:
`exp/quickanalyze3.py results/traceCombined-c2x4-rho0.80.result.<simtype>`

### single trace experiment (an example)
Note that single trace experiment is a special case of the multi-trace experiment. If you prefer,
however, you could run experiments with replayTrace.py.

* Set the simtype config parameter to one of "soft", "hard", or "none"
* Restart your YARN RM
* Start your schedpolserver:
` src/schedpolserver -c config/config-timex1-c2x4-g4-h6-rho0.70`
* Run the experiment:
`exp/replayTrace.py -c config/config-timex1-c2x6-g4-h6-rho0.70 -t traces/traceMPI-c2x6-rho0.70`
* copy/move the results.txt file to a corresponding results file:
`mv $HADOOP_HOME/results.txt results/traceMPI-c2x6-rho0.70.result.<simtype>`
* Analyze results:
`exp/quickanalyze3.py results/traceMPI-c2x6-rho0.70.result.<simtype>`

