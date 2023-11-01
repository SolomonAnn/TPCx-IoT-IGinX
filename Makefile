
SRC=$(PWD)/TPCx-IoT-SRC
ASSET=TPCx-IoT-Runtime-Suite

#
# AT LEAST ONCE NEED TO RUN WITHOUT -o SO THAT DEPENDENCIES CAN BE DOWNLOADED VIA MAVEN
# FOR SUBSEQUENT BUILDS OR TO ENSURE NO RANDOM INGREDIENTS DOWNLOADED CAN RUN WITH -o (offline).  
# Note that even copied dependences are pulled with "file://" urls and need to run without -o once
# CE personally prefers to do most development in -o mode to track when new downloads are required.
#
#OFFLINE = -o

#
# MAVEN HINTS
#
# if you get "https required" maven errors check your ~/.m2/settings.xml and make sure your
# maven repo links are set to https not http
#
# If you are behind a firewall, add maven proxy settings to your ~/.m2/settings.xml
# see https://maven.apache.org/guides/mini/guide-proxies.html
#
#

MAGIC= $(OFFLINE) -Dcheckstyle.skip -Dmaven.test.skip=true

#build: java_build

build: java_build setup copyroot copytpcx-iot copy-javabuild zip

zip:
	rm -f target/target.zip
	(cd target; zip -r target.zip *)

setup:
	rm -rf target
	mkdir target
	mkdir target/machbase_scripts
	mkdir target/tpcx-iot
	mkdir target/tpcx-iot/bin
	mkdir target/tpcx-iot/workloads

copyroot:
	cp EULA.txt                                 target
	cp ${ASSET}/Benchmark_Macros_Couchbase.sh   target
	cp ${ASSET}/Benchmark_Macros_Hbase.sh       target
	cp ${ASSET}/Benchmark_Macros_LindormTSDB.sh target
	cp ${ASSET}/Benchmark_Macros_Machbase.sh    target
	cp ${ASSET}/Benchmark_Parameters.sh         target
	cp ${ASSET}/client_driver_host_list.txt     target
	cp ${ASSET}/IoT_cluster_validate_suite.sh   target
	cp ${ASSET}/IoTDataCheck_Couchbase.sh       target
	cp ${ASSET}/IoTDataCheck_Hbase.sh           target
	cp ${ASSET}/IoTDataCheck_LindormTSDB.sh     target
	cp ${ASSET}/IoTDataCheck_Machbase.sh        target
	cp ${ASSET}/IoTDataRowCount_Couchbase.sh    target
	cp ${ASSET}/IoTDataRowCount_Hbase.sh        target
	cp ${ASSET}/IoTDataRowCount_LindormTSDB.sh  target
	cp ${ASSET}/IoTDataRowCount_Machbase.sh     target
	cp ${ASSET}/machbase_scripts/count_rows_in_table.sql target/machbase_scripts
	cp ${ASSET}/machbase_scripts/create_table.sql        target/machbase_scripts
	cp ${ASSET}/machbase_scripts/select_table.sql        target/machbase_scripts
	cp ${ASSET}/machbase_scripts/truncate_table.sql      target/machbase_scripts
	cp ${ASSET}/TPCx-IoT-client.sh            target
	cp ${ASSET}/TPCx-IoT-instances.sh         target
	cp ${ASSET}/TPCx-IoT-master.sh            target
	cp ${ASSET}/USER_GUIDE_NEW_BINDING.txt    target
	cp ${ASSET}/USER_GUIDE.txt                target

copytpcx-iot:
	cp ${ASSET}/tpcx-iot/LICENSE.txt              target/tpcx-iot
	cp ${ASSET}/tpcx-iot/NOTICE.txt               target/tpcx-iot
	cp ${ASSET}/tpcx-iot/bin/bindings.properties  target/tpcx-iot/bin
	cp ${ASSET}/tpcx-iot/bin/tpcx-iot             target/tpcx-iot/bin
	cp ${ASSET}/tpcx-iot/workloads/workloada               target/tpcx-iot/workloads
	cp ${ASSET}/tpcx-iot/workloads/workloadiot             target/tpcx-iot/workloads
	cp ${ASSET}/tpcx-iot/workloads/workloadiot.template    target/tpcx-iot/workloads
	cp ${ASSET}/tpcx-iot/workloads/workloadiot-warmup      target/tpcx-iot/workloads

copy-javabuild:
	(cd target/tpcx-iot; tar -zxvf ${SRC}/couchbase2/target/ycsb-couchbase2-binding-0.13.0-SNAPSHOT.tar.gz)
	mv target/tpcx-iot/ycsb-couchbase2-binding-0.13.0-SNAPSHOT target/tpcx-iot/couchbase2-binding
	(cd target/tpcx-iot; tar -zxvf ${SRC}/hbase10/target/ycsb-hbase10-binding-0.13.0-SNAPSHOT.tar.gz)
	mv target/tpcx-iot/ycsb-hbase10-binding-0.13.0-SNAPSHOT target/tpcx-iot/hbase10-binding
	(cd target/tpcx-iot; tar -zxvf ${SRC}/hbase12/target/ycsb-hbase12-binding-0.13.0-SNAPSHOT.tar.gz)
	mv target/tpcx-iot/ycsb-hbase12-binding-0.13.0-SNAPSHOT target/tpcx-iot/hbase12-binding
	(cd target/tpcx-iot; tar -zxvf ${SRC}/lindormtsdb/target/ycsb-lindormtsdb-binding-0.13.0-SNAPSHOT.tar.gz)
	mv target/tpcx-iot/ycsb-lindormtsdb-binding-0.13.0-SNAPSHOT target/tpcx-iot/lindormtsdb-binding
	(cd target/tpcx-iot; tar -zxvf ${SRC}/machbase/target/ycsb-machbase-binding-0.13.0-SNAPSHOT.tar.gz)
	mv target/tpcx-iot/ycsb-machbase-binding-0.13.0-SNAPSHOT target/tpcx-iot/machbase-binding

java_build:
	(cd ${SRC}; mvn ${MAGIC} -pl core -am clean package)
	(cd ${SRC}; mvn ${MAGIC} -pl hbase10 -am clean package)
	(cd ${SRC}; mvn ${MAGIC} -pl hbase12 -am clean package)
	(cd $(SRC); mvn ${MAGIC} -pl couchbase2 -am clean package)
	(cd $(SRC); mvn ${MAGIC} -pl machbase -am clean package)
	(cd $(SRC); mvn ${MAGIC} -pl lindormtsdb -am clean package)

clean: java_clean
	rm -rf target
	rm -rf ${SRC}/core/target
	rm -rf ${SRC}/hbase10/target
	rm -rf ${SRC}/hbase12/target
	rm -rf ${SRC}/couchbase2/target
	rm -rf ${SRC}/machbase/target
	rm -rf ${SRC}/lindormtsdb/target
	rm -rf ${SRC}/binding-parent/datastore-specific-descriptor/target

java_clean:
	(cd ${SRC}; mvn ${MAGIC} -pl core -am clean)
	(cd ${SRC}; mvn ${MAGIC} -pl hbase10 -am clean)
	(cd ${SRC}; mvn ${MAGIC} -pl hbase12 -am clean)
	(cd $(SRC); mvn ${MAGIC} -pl couchbase2 -am clean)
	(cd $(SRC); mvn ${MAGIC} -pl machbase -am clean)
	(cd $(SRC); mvn ${MAGIC} -pl lindormtsdb -am clean)

