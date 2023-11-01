# All the command below are specific for Lindorm TSDB please change as needed for other clients/databases

IOT_DATABASE="benchmark"
IOT_DATA_TABLE="sensor"

CHECK_IF_TABLE_EXISTS="exists table $IOT_DATA_TABLE;"

TRUNCATE_TABLE="drop database $IOT_DATABASE; create database $IOT_DATABASE with (string_compression = 'true', skip_wal = 'true', shard_num = 200); use $IOT_DATABASE; create table $IOT_DATA_TABLE (device_id VARCHAR TAG, time BIGINT, field0 VARCHAR, PRIMARY KEY (device_id));"

CREATE_TABLE="create database $IOT_DATABASE with (string_compression = 'true', skip_wal = 'true', shard_num = 200); use $IOT_DATABASE; create table $IOT_DATA_TABLE (device_id VARCHAR TAG, time BIGINT, field0 VARCHAR, PRIMARY KEY (device_id));"

CHECK_STATS_DB="show databases;"

COUNT_ROWS_IN_TABLE="use $IOT_DATABASE; stats table $IOT_DATA_TABLE;"

SUT_TABLE_PATH="/tsdb/data/*/default/$IOT_DATABASE*/*/chunks/0/*"

ROW_COUNT="ROWS="

DB_HOST="tsdb-1"

DB_PORT="8242"

SUT_SHELL="lindorm-cli -url jdbc:lindorm:tsdb:url=http://$DB_HOST:$DB_PORT"

# limdorm.tsdb.hosts : Lindorm TSDB connection string or lists of cluster nodes, separated by ":"
SUT_PARAMETERS="lindorm.tsdb.hosts=tsdb-1:tsdb-2:tsdb-3:tsdb-4:tsdb-5:tsdb-6:tsdb-7:tsdb-8:tsdb-9:tsdb-10,lindom.tsdb.port=$DB_PORT,lindorm.tsdb.batchsize=18000,lindorm.tsdb.database=$IOT_DATABASE,lindorm.tsdb.table=$IOT_DATA_TABLE,lindorm.tsdb.driver.http.compression=true,lindorm.tsdb.debug=false"

SUDO="sudo"
