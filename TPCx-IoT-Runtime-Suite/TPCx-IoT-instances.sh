#!/bin/bash
# Usage: ./tpcx-iot-instances.sh 1000 4 100

# Usage check

echo ">>>>>>>>> entering instances"

counter=1
recordCount=$1
totalOperationCount=$2
numInstances=$3
threadCount=$4
start=$5
clientID=$6
DATABASE_CLIENT=$7
PWD=$8
SUT_PARAMETERS=$9
RUN_TYPE=${10}

operationCount=$((totalOperationCount / numInstances))  # Improve this to be total of record count
echo "instance Operation: $operationCount"
#threadCount=$((totalThreadCount / numInstances))
echo "instance Tread: $threadCount"

while [ $counter -le $numInstances ]
do

echo $counter

cat << EOF | tee ./tpc_iot_instance${counter}_workload
insertstart=$start
insertcount=$operationCount
recordcount=$recordCount
operationcount=$operationCount
workload=com.yahoo.ycsb.workloads.CoreWorkload
readallfields=true
readproportion=0.0
updateproportion=0.0
# scanproportion=0
insertproportion=1
threadcount=$threadCount
requestdistribution=uniform
EOF

#echo "./tpcx-iot/bin/tpcx-iot load basic -P ./tpc_iot_instance${counter}_workload -s > /dev/shm/large$counter.dat"
#nohup ./tpcx-iot/bin/tpcx-iot load basic -P ./tpc_iot_instance${counter}_workload -s > /dev/null &

echo "./tpcx-iot/bin/tpcx-iot run $DATABASE_CLIENT -P ./tpc_iot_instance${counter}_workload -p $SUT_PARAMETERS -p client=$clientID${counter} -p runtype=$RUN_TYPE -s > $PWD/logs/db$RUN_TYPE$counter.dat &"
nohup $PWD/tpcx-iot/bin/tpcx-iot run $DATABASE_CLIENT -P ./tpc_iot_instance${counter}_workload -p $SUT_PARAMETERS -p client=$clientID${counter} -p runtype=$RUN_TYPE -s > $PWD/logs/db$RUN_TYPE$counter.dat &
#nohup $PWD/tpcx-iot/bin/tpcx-iot run $DATABASE_CLIENT -P ./tpc_iot_instance${counter}_workload -p columnfamily=cf -s > $PWD/logs/db$counter.dat &
pids="$pids $!"

start=$((start + operationCount))

((counter++))

done
echo "instaces pids = $pids"
wait $pids
echo All done

