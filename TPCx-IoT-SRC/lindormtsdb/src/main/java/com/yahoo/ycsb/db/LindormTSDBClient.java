/**
 * Copyright (c) 2013 - 2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import java.io.IOException;
import java.io.StringReader;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A class that wraps the LindormTSDBClient to allow it to be interfaced with YCSB.
 * This class extends {@link DB} and implements the database interface used by YCSB client.
 */

/**
 * Lindorm TSDB Client for YCSB framework.
 * This class extends {@link DB} and implements the database interface used by YCSB client.
 */
public class LindormTSDBClient extends DB {
    public static final String CONFIG_ENTRIES_SEPARATOR = ",";
    public static final String LINDORM_TSDB_HOSTS_SEPARATOR = ":";

    /**
     * Lindorm TSDB JDBC Driver Configurations
     */
    public static final String LINDORM_TSDB_URL = "lindorm.tsdb.url";
    public static final String LINDORM_TSDB_HOSTS = "lindorm.tsdb.hosts";
    public static final String LINDORM_TSDB_PORT = "lindorm.tsdb.port";
    public static final String LINDORM_TSDB_DATABASE = "lindorm.tsdb.database";
    public static final String LINDORM_TSDB_DRIVER_CONNECTION_TIMEOUT = "lindorm.tsdb.driver.connect.timeout";
    public static final String LINDORM_TSDB_DRIVER_SOCKET_TIMEOUT = "lindorm.tsdb.driver.socket.timeout";
    public static final String LINDORM_TSDB_DRIVER_COMPRESSION_ENABLED = "lindorm.tsdb.driver.http.compression";

    /**
     * Lindorm TSDB Client Config Parameters
     */
    public static final String LINDORM_TSDB_TABLE ="lindorm.tsdb.table";
    public static final String INSERTS_BATCH_SIZE = "lindorm.tsdb.batchsize";
    public static final String LINDORM_TSDB_DEBUG_MODE = "lindorm.tsdb.debug";
    public static Boolean DEBUG_ENABLED = false;

    public static final String PRIMARY_KEY_NAME = "device_id";

    private Properties clientProperties;

    /**
     * Lindorm TSDB Client Default Configurations
     */
    public static final Integer DEFAULT_BATCH_SIZE = 3000;
    public static final String DEFAULT_CONNECTION_TIMEOUT = "30000";
    public static final String DEFAULT_SOCKET_TIMEOUT = "30000";
    public static final String DEFAULT_LINDORM_TSDB_PORT = "8242";
    public static final String DEFAULT_LINDORM_TSDB_DATABASE = "benchmark";
    public static final String DEFAULT_DATA_TABLE = "sensor";

    private int batchSize = 0;
    private long numRowsInBatch = 0L;
    private long insertTimestamp = 0L;

    /**
     * Lindorm TSDB Connections
     */
    private String tableName;
    private Connection queryConn = null;
    private PreparedStatement preparedQueryStmt = null;
    private Connection insertConn;
    private PreparedStatement preparedInsertStmt;

    private int queryExecuted = 0;
    private int queryFailed = 0;
    private int queryEmptyResult = 0;

    private static final AtomicInteger hostNum = new AtomicInteger(0);
    private String host;

    public LindormTSDBClient() {
        super();
    }

    /**
     * Extract Lindorm TSDB client configuration from $SUT_PARAMETERS variable.
     *
     * @param configsFromString
     * @return
     * @throws IOException
     */
    private Properties buildClientProperties(String configsFromString) throws IOException {
        Properties clientProperties = new Properties();
        clientProperties.load(new StringReader(configsFromString.replaceAll(CONFIG_ENTRIES_SEPARATOR, "\n")));
        printDebugMsg("Lindorm TSDB Client Properties : " + clientProperties.toString());
        return clientProperties;
    }

    private String buildClientConnectionOptions() {
        String socketTimeout = clientProperties.getProperty(LINDORM_TSDB_DRIVER_SOCKET_TIMEOUT, DEFAULT_SOCKET_TIMEOUT);
        String connectionTimeout = clientProperties.getProperty(LINDORM_TSDB_DRIVER_CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
        String compression = clientProperties.getProperty(LINDORM_TSDB_DRIVER_COMPRESSION_ENABLED, "false");
        String database = clientProperties.getProperty(LINDORM_TSDB_DATABASE, DEFAULT_LINDORM_TSDB_DATABASE);
        return ";database=" + database + ";"
                + LINDORM_TSDB_DRIVER_SOCKET_TIMEOUT + "=" + socketTimeout + ";"
                + LINDORM_TSDB_DRIVER_CONNECTION_TIMEOUT + "=" + connectionTimeout + ";"
                + LINDORM_TSDB_DRIVER_COMPRESSION_ENABLED + "=" + compression;
    }

    private String sqlClientURLSetup() {
        String configedHost = clientProperties.getProperty(LINDORM_TSDB_HOSTS, null);
        int hostIndex = hostNum.getAndIncrement();
        if (configedHost != null && !configedHost.isEmpty()) {
            String[] hosts = configedHost.split(LINDORM_TSDB_HOSTS_SEPARATOR);
            this.host = hosts[hostIndex % hosts.length];
        } else {
            throw new IllegalArgumentException("Invalid Lindorm TSDB hosts. Cannot connect to Lindorm TSDB cluster.");
        }
        Thread.currentThread().setName(host + "-" + hostIndex);
        String port = clientProperties.getProperty(LINDORM_TSDB_PORT, DEFAULT_LINDORM_TSDB_PORT);

        String clientConnectionOpts = buildClientConnectionOptions();
        String url = clientProperties.getProperty(LINDORM_TSDB_URL, "jdbc:lindorm:tsdb:url=http://" + host + ":" + port);
        url = url + clientConnectionOpts;
        printDebugMsg("Lindorm TSDB JDBC Connection String : " + url);
        return url;
    }

    private Connection initializeConnection() throws Exception {
        String url = sqlClientURLSetup();
        return DriverManager.getConnection(url, clientProperties);
    }

    private void prepareInsertStatement() throws Exception {
        this.tableName = clientProperties.getProperty(LINDORM_TSDB_TABLE, DEFAULT_DATA_TABLE);
        // Get table schema and prepare insert statement
        String describeTable = "describe table " + this.tableName;
        Statement describeStmt = queryConn.createStatement();
        ResultSet tableSchema = describeStmt.executeQuery(describeTable);
        int columns = 0;
        StringBuilder preparedVariables = new StringBuilder();
        StringBuilder preparedColumns = new StringBuilder();
        while(tableSchema.next()){
            columns++;
            if (columns == 1) {
                preparedColumns.append(tableSchema.getString(1));
                preparedVariables.append("?");
            } else {
                preparedColumns.append(",");
                preparedColumns.append(tableSchema.getString(1));
                preparedVariables.append(",?");
            }
        }
        tableSchema.close();
        describeStmt.close();

        String insertSQL = "insert into " + this.tableName + "(" + preparedColumns.toString() + ") values (" + preparedVariables.toString() + ")";
        printDebugMsg("Prepared Insert Statement = " + insertSQL);
        preparedInsertStmt = insertConn.prepareStatement(insertSQL);
    }

    private void initializeServerConnection() throws Exception {
        Class.forName("com.aliyun.lindorm.tsdb.client.Driver");

        try {
            this.insertConn = initializeConnection();
        } catch (Exception ex) {
            printMsg("ERROR! Failed to initialize Lindorm TSDB insert connection.", ex);
            throw ex;
        }

        try {
            this.queryConn = initializeConnection();
        } catch (Exception ex) {
            printMsg("ERROR! Failed to initialize Lindorm TSDB query connection.", ex);
            throw ex;
        }
    }

    @Override
    public void init() {
        // Load and parse Lindorm TSDB client configuration
        String clientPropertiesStr = getProperties().toString();
        try {
            this.clientProperties = buildClientProperties(clientPropertiesStr);
        } catch (Exception ex) {
            printMsg("WARNING! Exception encounter during parsing input Lindorm TSDB client properties. " +
                    "Use default values.", ex);
        }

        String batchSizeStr = clientProperties.getProperty(INSERTS_BATCH_SIZE, DEFAULT_BATCH_SIZE.toString());
        if (batchSizeStr != null) {
            try {
                this.batchSize = Integer.parseInt(batchSizeStr);
            } catch (NumberFormatException nfex) {
                printMsg("WARNING! Invalid lindorm.tsdb.batchsize specified. Use default batch size.", nfex);
                this.batchSize = DEFAULT_BATCH_SIZE;
            }
        } else {
            this.batchSize = DEFAULT_BATCH_SIZE;
        }

        String debugStr = clientProperties.getProperty(LINDORM_TSDB_DEBUG_MODE, "false");
        if (debugStr != null) {
            DEBUG_ENABLED = Boolean.parseBoolean(debugStr);
        }

        Properties systemProperties = System.getProperties();
        System.setProperties(systemProperties);

        // Initialize insert & query connections
        try {
            initializeServerConnection();
        } catch (Exception ex) {
            printMsg("ERROR! Exception encountered during Lindorm TSDB JDBC connections setup.", ex);
            throw new RuntimeException(ex);
        }

        // Prepare insert statement
        // Note : We will prepare query statement during query execution time.
        try {
            prepareInsertStatement();
        } catch (Exception ex) {
            printMsg("ERROR! Exception encountered during insert preparedStatement setup.", ex);
            throw new RuntimeException(ex);
        }
    }

    private void printDebugMsg(String debugMsg) {
        if (DEBUG_ENABLED) {
            printMsg(debugMsg, null);
        }
    }

    private String getCurrentTimestamp() {
        long timestamp = System.currentTimeMillis();
        SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm:ss");
        java.util.Date data = new Date(timestamp);
        return sdf.format(data);
    }

    private void printMsg(String msg, Exception ex) {
        String timestamp = getCurrentTimestamp();
        String threadInfo = "[" + Thread.currentThread().getName() + "] : ";
        System.out.println(threadInfo + " : "
                + timestamp + " : "
                + msg
                + (ex != null ? " | Exception Msg : " + ex.getMessage() : ""));
        if (ex != null) {
            ex.printStackTrace(System.out);
        }
    }

    /**
     * Shutdown the client.
     */
    @Override
    public void cleanup() {
        printDebugMsg("###### Query Execution Stats :"
                + " 1. Query Executed : " + queryExecuted
                + " 2. Query Failed : " + queryFailed
                + " 3. Query (Empty) : " + queryEmptyResult
                + " ######");

        try {
            if (this.preparedQueryStmt != null) {
                this.preparedQueryStmt.close();
                this.preparedQueryStmt = null;
            }
            if (this.preparedInsertStmt != null) {
                this.preparedInsertStmt.close();
                this.preparedInsertStmt = null;
            }

            if (this.insertConn != null) {
                this.insertConn.close();
                this.insertConn = null;
            }

            if (this.queryConn != null) {
                this.queryConn.close();
                this.queryConn = null;
            }
        } catch (Exception ex) {
            printMsg("ERROR! Exception encountered during cleanup.", ex);
        }
    }

    /**
     * Not Supported.
     * Use scan() for YCSB test.
     */
    @Override
    public Status read(final String table, final String key, final Set<String> fields, final HashMap<String, ByteIterator> result) {
        return Status.OK;
    }

    private static String collectFields(final Set<String> fields) {
        if (fields == null || fields.isEmpty()) {
            return "*";
        }

        StringBuilder builder = new StringBuilder();
        for (String field : fields) {
            builder.append(field).append(",");
        }

        String toReturn = builder.toString();

        // Remove trailing ","
        return toReturn.substring(0, toReturn.length() - 1);
    }

    @Override
    public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                       final Vector<HashMap<String, ByteIterator>> result) {
        String sqlQuery = "SELECT " + collectFields(fields) + " FROM " + table + " WHERE " + PRIMARY_KEY_NAME
                + " = " + startkey;
        if (recordcount > 0) {
            sqlQuery += " limit " + recordcount;
        }

        try {
            if (queryConn == null || queryConn.isClosed()) {
                this.queryConn = initializeConnection();
                printMsg( "WARNING! Query connection has been re-initialized.", null);
            }

            preparedQueryStmt = queryConn.prepareStatement(sqlQuery);
            queryExecuted++;

            long queryStart = System.currentTimeMillis();
            ResultSet queryResult = preparedQueryStmt.executeQuery();
            ResultSetMetaData metaData = queryResult.getMetaData();

            HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>(queryResult.getRow());
            for (int columnIndex = 1; columnIndex <= metaData.getColumnCount(); columnIndex++) {
                tuple.put(metaData.getColumnName(columnIndex), new StringByteIterator(queryResult.getString(columnIndex)));
                result.add(tuple);
            }

            queryResult.close();
            preparedQueryStmt.close();
            preparedQueryStmt = null;

            long queryCost = System.currentTimeMillis() - queryStart;
            if (queryCost > 2000) {
                printDebugMsg("WARNING! Query Cost " + queryCost + "ms | Query Result : " + result.size() +
                        " | Detailed Query : " + sqlQuery);
            }

            if (result.size() == 0) {
                queryEmptyResult++;
                printDebugMsg("Empty Scan Result for " + sqlQuery + " | Query Cost in ms : " + queryCost);
            } else {
                printDebugMsg("Successfully queried " + result.size()
                        + " dataset for " + sqlQuery + " | Query Cost in ms : " + queryCost);
            }
            return Status.OK;
        } catch (Exception ex) {
            queryFailed++;
            printMsg("ERROR! Exception encountered while running query : start key = " + startkey + " count = " + recordcount, ex);
            return Status.ERROR;
        }
    }

    /**
     * Not Supported.
     * Use insert to cover previous value.
     */
    @Override
    public Status update(final String table, final String key, final HashMap<String, ByteIterator> values) {
        return Status.OK;
    }

    @Override
    public Status insert(final String table, final String key, final HashMap<String, ByteIterator> values) {
        try {
            long addBatchStart = System.currentTimeMillis();
            // Key : device_id:timestamp
            String[] splitKeys = key.split(":");
            String deviceId = splitKeys[0] + ":" + splitKeys[1];
            preparedInsertStmt.setString(1, deviceId);
            long timestamp = Long.parseLong(splitKeys[2]);
            preparedInsertStmt.setLong(2, timestamp);
            for (Map.Entry<String, ByteIterator> value : values.entrySet()) {
                preparedInsertStmt.setString(3, value.getValue().toString());
            }
            preparedInsertStmt.addBatch();
            numRowsInBatch++;
            long batchAddCost = System.currentTimeMillis() - addBatchStart;
            if (batchAddCost > 200) {
                printDebugMsg("WARNING! Batch addition operation costs more than 200ms - " + batchAddCost);
            }

            // Sync insert requests in batch
            if (numRowsInBatch % this.batchSize == 0) {
                long executeBatchStart = System.currentTimeMillis();
                preparedInsertStmt.executeBatch();
                long executeBatchCost = System.currentTimeMillis() - executeBatchStart;
                if (executeBatchCost > 1000L) {
                    printDebugMsg("WARNING! Batch execution costs more than 1000ms - " + executeBatchCost);
                }
            }

            insertTimestamp = System.currentTimeMillis();
            return Status.OK;
        } catch (Exception ex) {
            if (numRowsInBatch % this.batchSize != 0) {
                printMsg("ERROR encountered during insert batch preparation. Failed with : " + key, ex);
            } else {
                printMsg("ERROR encountered during batch execution. Failed " + this.batchSize + " insert ops.", ex);
            }
            return Status.ERROR;
        }
    }

    /**
     * Not Supported
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return
     */
    @Override
    public Status delete(final String table, final String key) {
        return Status.OK;
    }

    /**
     *
     * @param table
     * @param filter Sensor Name
     * @param clientFilter Client-Driver Id
     * @param timestamp Record timestamp
     * @param fields fields are queried
     * @param runStartTime
     * @param result1 Data Structure for results from query 1
     * @param result2 Data Structure for results from query 2
     * @return
     */
    @Override
    public Status scan(String table,
                       String filter,
                       String clientFilter,
                       String timestamp,
                       Set<String> fields,
                       long runStartTime,
                       Vector<HashMap<String, ByteIterator>> result1,
                       Vector<HashMap<String, ByteIterator>> result2) {
        long oldTimeStamp;
        long startTime = System.currentTimeMillis();

        // First Query Request
        long longTimestamp = Long.parseLong(timestamp);
        Status scanStatus1 = scanHelper(table, filter, clientFilter, longTimestamp, fields, result1);

        // Second Query Request
        if (runStartTime > 0L) {
            long time = longTimestamp - runStartTime;
            oldTimeStamp = longTimestamp - time;
        } else {
            oldTimeStamp = longTimestamp - 1800000L;
        }
        long timestampVal = oldTimeStamp + (long) (Math.random() * (startTime - 10000L - oldTimeStamp));
        Status scanStatus2 = scanHelper(table, filter, clientFilter, timestampVal, fields, result2);

        // Result sanity check
        if (scanStatus1.isOk() && scanStatus2.isOk()) {
            if (result1.size() == 0 || result2.size() == 0) {
                // Empty Result
                printDebugMsg("Empty query result for Query : { table = " + table
                        + "; filter = " + filter
                        + "; clientFilter = " + clientFilter
                        + "; timestamp = " + timestamp
                        + "; runStartTime = " + runStartTime + " } | "
                        + "Result #1 : " + result1.size()
                        + "; Result #2 : " + result2.size());
            }
            return Status.OK;
        } else {
            printMsg("ERROR encountered while processing queries. Scan #1 Status : " + scanStatus1.isOk()
                    + " | Scan #2 Status : " + scanStatus2.isOk(), null);
            return Status.ERROR;
        }
    }

    private Status scanHelper(String table, String filter, String clientFilter, long timestamp,
                              Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        String deviceId = clientFilter + ":" + filter;
        String queryFields = collectFields(fields);

        try {
            if (queryConn == null || queryConn.isClosed()) {
                this.queryConn = initializeConnection();
                printMsg("WARNING! Query connection has been re-initialized.", null);
            }
        } catch (Exception ex) {
            queryFailed++;
            printMsg("ERROR! Exception encountered while re-initializing query connection.", ex);
            queryConn = null;
            return Status.ERROR;
        }

        String sqlQueryStr = "SELECT " + queryFields
                + " FROM " + this.tableName + " WHERE " + PRIMARY_KEY_NAME + " = '" + deviceId + "' and time "
                + " between " + timestamp + " and " + (timestamp + 5000L);

        try {
            // Prepare and execute SQL query
            long queryExecStart = System.currentTimeMillis();
            Statement queryStatement = queryConn.createStatement();

            ResultSet queryResult = queryStatement.executeQuery(sqlQueryStr);
            ResultSetMetaData metaData = queryResult.getMetaData();
            queryExecuted++;

            // Parse query result
            while (queryResult.next()) {
                HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>();
                for (int columnIndex = 1; columnIndex <= metaData.getColumnCount(); columnIndex++) {
                    tuple.put(metaData.getColumnName(columnIndex).toLowerCase(),
                            new StringByteIterator(queryResult.getString(columnIndex)));
                    result.add(tuple);
                }
            }

            if (result.isEmpty()) {
                queryEmptyResult++;
            }

            // Close statement and result set.
            queryResult.close();
            queryStatement.close();

            long queryCost = System.currentTimeMillis() - queryExecStart;
            if (queryCost > 2000) {
                printDebugMsg("WARNING! Query cost " + queryCost + "ms | Query Result : " + result.size() +
                        " | Detailed Query : " + sqlQueryStr);
            }

            return Status.OK;
        } catch (Exception ex) {
            queryFailed++;
            printMsg("ERROR! Exception encountered while running query (" + sqlQueryStr +").", ex);
            return Status.ERROR;
        }
    }
}
