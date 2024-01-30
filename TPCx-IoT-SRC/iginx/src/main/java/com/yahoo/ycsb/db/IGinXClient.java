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

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.pool.SessionPool;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.thrift.DataType.BINARY;

public class IGinXClient extends DB {

    private SessionPool sessionPool = null;

    private static final Object CACHE_LOCK = new Object();
    private static final String DEFAULT_IGINX_INFO = "172.16.17.21:6888,172.16.17.22:6888,172.16.17.23:6888,172.16.17.24:6888";
    private static Map<Long, Map<String, byte[]>> cacheData;
    private static int cacheNum = 0;
    private static String measurement;

    private String dbHost = "localhost";
    private int dbPort = 6888;
    private static final int CACHE_THRESHOLD = 10000;

    public Set<String> getLocalIps() {
        Set<String> ips = new HashSet<>();
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            InetAddress ip;
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaces.nextElement();
                if (networkInterface.isLoopback() || networkInterface.isVirtual() || !networkInterface.isUp()) {
                    continue;
                }
                Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    ip = addresses.nextElement();
                    if (ip instanceof Inet4Address) {
                        ips.add(ip.getHostAddress());
                    }
                }
            }
        } catch (Exception e) {
            System.err.printf("get error when loading local ips: %s%n", e);
        }
        return ips;
    }

    @Override
    public void init() throws DBException {
        String param;
        if (!getProperties().containsKey("iginxinfo") || getProperties().getProperty("iginx") == null) {
            param = DEFAULT_IGINX_INFO;
            System.err.printf("unable to load iginxinfo, use %s as default%n", param);
        } else {
            param = getProperties().getProperty("iginxinfo");
        }
        Set<String> localIps = getLocalIps();
        String[] serversInfo = param.split(",");
        List<String> localServers = new ArrayList<>();
        for (String serverInfo: serversInfo) {
            for(String localIp: localIps) {
                if (serverInfo.contains(localIp)) {
                    localServers.add(serverInfo);
                    break;
                }
            }
        }
        Random random = new Random(0);
        String[] serverInfo;
        String server;
        if (localServers.isEmpty()) { // 没有与本机在同一个节点的 iginx
            server = serversInfo[random.nextInt(serversInfo.length)];
        } else {
            server = localServers.get(random.nextInt(localServers.size()));
        }
        serverInfo = server.split(":");

        if (serverInfo.length != 2) {
            System.err.println("Parse IGinX Server info failed,it should be ip:port");
        } else {
            this.dbHost = serverInfo[0];
            this.dbPort = Integer.parseInt(serverInfo[1]);
        }
        SessionException sessionException = null;
        synchronized (CACHE_LOCK) {
            if (cacheData == null) {
                cacheData = new HashMap<>();
            }
            if (sessionPool == null) {
                sessionPool = new SessionPool(dbHost, dbPort, "root", "root", 100);
                System.err.printf("start session(%s:%s) succeed%n", dbHost, dbPort);
            }
        }
        if (sessionException != null) {
            throw new DBException(sessionException);
        }
    }

    @Override
    public void cleanup() throws DBException {
        try {
            synchronized (CACHE_LOCK) {
                if (cacheNum > 0) {
                    insertRecords(cacheData);
                    cacheData.clear();
                }
            }
            sessionPool.close();
            sessionPool = null;
        } catch (SessionException | ExecutionException e) {
            System.err.printf("cleanup session(%s:%s) failed:%s%n", dbHost, dbPort, e);
            e.printStackTrace();
            throw new DBException(e);
        }
    }

    @Override
    public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        return Status.OK;
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return Status.OK;
    }

    private Status scanHelper(String deviceId, long timestamp, Set<String> fields,
                              Vector<HashMap<String, ByteIterator>> result)
            throws ExecutionException {
        long startTime = timestamp;
        long endTime = timestamp + 5000L;
        List<String> paths = Collections.singletonList(deviceId + ".field0");
        try {
            SessionQueryDataSet dataSet;
            dataSet = sessionPool.queryData(paths, startTime, endTime);
            for (int i = 0; i < dataSet.getKeys().length; i++) {
                HashMap<String, ByteIterator> rowResult = new HashMap<>();
                if (dataSet.getPaths().size() != 0) {
                    rowResult
                            .put("field0", new ByteArrayByteIterator((byte[]) dataSet.getValues().get(i).get(0)));
                    result.add(rowResult);
                }
            }
        } catch (SessionException e) {
            e.printStackTrace();
            return Status.ERROR;
        }
        return Status.OK;
    }

    @Override
    public Status scan(String table, String key, String client, String timestamp, Set<String> fields, long runStartTime, Vector<HashMap<String, ByteIterator>> result1, Vector<HashMap<String, ByteIterator>> result2) {
        String deviceId = String.format("%s.%s", client, key);
        long newTimeStamp = Long.parseLong(timestamp);
        long oldTimeStamp;
        Status s1 = null;
        try {
            s1 = scanHelper(deviceId, newTimeStamp, fields, result1);
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
//        if (runStartTime > 0L) {
//            long time = newTimeStamp - runStartTime;
//            oldTimeStamp = newTimeStamp - time;
//        } else {
//            oldTimeStamp = newTimeStamp - 1800000L;
//        }
//        long timestampVal =
//                oldTimeStamp + (long) (Math.random() * (newTimeStamp - 10000L - oldTimeStamp));
//        timestampVal = Math.max(0L, timestampVal);
//        Status s2 = null;
//        try {
//            s2 = scanHelper(deviceId, timestampVal, fields, result2);
//        } catch (ExecutionException e) {
//            e.printStackTrace();
//        }
//        if ((s1 != null && s1.isOk()) && (s2 != null && s2.isOk())) {
//            return Status.OK;
//        }
        if (s1 != null && s1.isOk()) {
            return Status.OK;
        }
        return Status.ERROR;
    }

    @Override
    public Status update(String table, String key, HashMap<String, ByteIterator> values) {
        return Status.OK;
    }

    private void insertRecords(Map<Long, Map<String, byte[]>> cacheData)
            throws SessionException, ExecutionException {
        List<String> prefixList = cacheData.values().stream().map(Map::keySet)
                .flatMap(Collection::stream).distinct().collect(Collectors.toList());
        List<String> paths = new ArrayList<>();
        List<DataType> dataTypeList = new ArrayList<>();
        long[] timestamps = new long[cacheData.size()];
        Map<String, Object[]> valuesMap = new LinkedHashMap<>();
        for (String prefix : prefixList) {
            paths.add(prefix + "." + measurement);
            dataTypeList.add(BINARY);
            valuesMap.put(prefix, new Object[timestamps.length]);
        }
        int index = 0;
        for (Map.Entry<Long, Map<String, byte[]>> entry : cacheData.entrySet()) {
            timestamps[index] = entry.getKey();
            Map<String, byte[]> fieldMap = entry.getValue();
            for (Map.Entry<String, byte[]> e : fieldMap.entrySet()) {
                valuesMap.get(e.getKey())[index] = e.getValue();
            }
            index++;
        }
        Object[] valuesList = new Object[paths.size()];
        int i = 0;
        for (Object values : valuesMap.values()) {
            valuesList[i] = values;
            i++;
        }
        sessionPool.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
    }

    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        String[] params = key.split(":");
        String deviceId = String.format("%s.%s", params[0], params[1]);
        long timestamp = Long.parseLong(params[2]);

        try {
            Map<Long, Map<String, byte[]>> previousCachedData = null;
            if (measurement == null) {
                measurement = values.keySet().iterator().next();
            }
            byte[] cValue = values.values().iterator().next().toArray();
            synchronized (CACHE_LOCK) {
                cacheData.computeIfAbsent(timestamp, k -> new HashMap<>()).put(deviceId, cValue);
                cacheNum++;
                if (cacheNum >= CACHE_THRESHOLD) {
                    previousCachedData = cacheData;
                    cacheNum = 0;
                    cacheData = new HashMap<>();
                }
            }
            if (previousCachedData != null) {
                insertRecords(previousCachedData);
            }
        } catch (SessionException | ExecutionException e) {
            e.printStackTrace();
            System.err
                    .printf("write %d records to server failed because %s%n", CACHE_THRESHOLD, e.toString());
            return Status.ERROR;
        }
        return Status.OK;
    }

    @Override
    public Status delete(String table, String key) {
        return Status.OK;
    }
}