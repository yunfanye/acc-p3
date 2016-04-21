export LD_LIBRARY_PATH=/usr/local/lib/
export HADOOP_HOME=/opt/projects/advcc/hadoop/hadoop-2.2.0; export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop; export HADOOP_PREFIX=$HADOOP_HOME; export HADOOP_YARN_HOME=$HADOOP_HOME; export JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64;

cp ../results.txt.bak ../results.txt

exp/replayAll.py -c config/config-timex1-c2x4-g4-h6-rho0.70 -t traces/traceMPI-c2x4-rho0.80,traces/traceGPU-c2x4-rho0.80

a=`wc -l ../results.txt | awk -F' ' '{print $1}'`
while (($a < 58)); do echo 'ha'; sleep 10; a=`wc -l ../results.txt | awk -F' ' '{print $1}'`; done


cp ../results.txt traceCombined-c2x4-rho0.80.result.soft

echo "FINISH!!!!!!!!!"

