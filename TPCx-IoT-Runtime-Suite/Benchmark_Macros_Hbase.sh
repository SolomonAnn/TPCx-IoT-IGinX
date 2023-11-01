# All the command below are specific for HBase please change as needed for other clients/databases

CHECK_IF_TABLE_EXISTS="exists 'usertable'"

TRUNCATE_TABLE="truncate 'usertable'"

CREATE_TABLE="n_splits=40; create 'usertable', 'cf', {SPLITS => (1..n_splits).map {|i| \"rhel#{10+i*(90-10)/n_splits}\"}}"

CHECK_STATS_DB="status 'simple'"

#COUNT_ROWS_IN_TABLE="count 'usertable', INTERVAL=>1000000"

COUNT_ROWS_IN_TABLE="hbase org.apache.hadoop.hbase.mapreduce.RowCounter usertable"

SUT_TABLE_PATH="/hbase/data/default/usertable/*/.regioninfo"

ROW_COUNT="ROWS="

SUT_SHELL="hbase shell"

IOT_DATA_TABLE=usertable

SUT_PARAMETERS="columnfamily=cf"
