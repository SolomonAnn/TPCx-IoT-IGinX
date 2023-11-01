<!--
Copyright (c) 2015-2017 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

# Lindorm TSDB Driver for YCSB
This driver is a binding for the YCSB facilities to operate against a Lindorm TSDB Server cluster. It uses the official Lindorm JDBC Driver and provides a rich set of configuration options.

## Quickstart

### 1. Start Lindorm TSDB Server
You need to start a single node or a cluster to point the client at. Please see [https://www.alibabacloud.com/help/en/doc-detail/182269.html](Alibaba Cloud Lindorm TSDB) for more details and instructions.

### 2. Setup Lindorm-Cli Tool 
Lindorm-Cli tool is the command-line interface to interact with Lindorm TSDB Server cluster.
Please see [https://help.aliyun.com/document_detail/216787.html](Lindorm-Cli) for more detailed instructions and commands.

Connect with Lindorm TSDB Server cluster:
```
./lindorm-cli -url http://ld-xxxx-proxy-tsdb-pub.lindorm.rds.aliyuncs.com:8242
```

### 3. Setup Lindorm TSDB database and table with Lindorm-Cli Tool
```
lindorm> create database benchmark with (string_compression = 'true', shard_num = 200);
lindorm> use benchmark;
lindorm:benchmark> create table sensor (device_id VARCHAR PRIMARY TAG, time BIGINT, field0 VARCHAR);
```
Note: Database setup is optional. Default database is available to use. However, default database is not tuned for performance.

### 4. Set up YCSB
You need to clone the repository and compile everything.

```
git clone git://github.com/brianfrankcooper/YCSB.git
cd YCSB
mvn clean package
```

### 5. Run the Workload
Before you can actually run the workload, you need to "load" the data first.

```
bin/ycsb load lindormtsdb -s -P workloads/workloada
```

Then, you can run the workload:

```
bin/ycsb run lindormtsdb -s -P workloads/workloada
```

## Configuration Options
Since no setup is the same and the goal of YCSB is to deliver realistic benchmarks, here are some setups that you can
tune. Note that if you need more flexibility (let's say a custom transcoder), you still need to extend this driver and
implement the facilities on your own.

You can set the following properties (with the default settings applied):

- lindom.tsdb.hosts=tsdb-1:tsdb-2:tsdb-3 => Lindorm TSDB cluster hostnames/IPs, separated by ":" or Lindorm TSDB connection string.
- lindom.tsdb.port=8242 => Lindorm TSDB cluster service port.
- lindorm.tsdb.batchsize=3000 => Insert workload batch processing size.
- lindorm.tsdb.database=default => Lindorm TSDB database.
- lindorm.tsdb.table=sensor => Lindorm TSDB data table.
- lindorm.tsdb.debug=false => Enable debug messages.
- lindorm.tsdb.driver.http.compression=false => Enable client http gzip compression to save network bandwidth.