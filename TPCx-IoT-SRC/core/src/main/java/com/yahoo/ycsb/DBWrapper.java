/**
 * Copyright (c) 2010 Yahoo! Inc., 2016-2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb;

import com.yahoo.ycsb.measurements.Measurements;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;


import java.util.*;
import java.util.logging.Logger;

/**
 * Wrapper around a "real" DB that measures latencies and counts return codes.
 * Also reports latency separately between OK and failed operations.
 */
public class DBWrapper extends DB {
  private final DB db;
  private final Measurements measurements;
  private final Tracer tracer;

  private boolean reportLatencyForEachError = false;
  private HashSet<String> latencyTrackedErrors = new HashSet<String>();

  private static final String REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY = "reportlatencyforeacherror";
  private static final String REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY_DEFAULT = "false";

  private static final String LATENCY_TRACKED_ERRORS_PROPERTY = "latencytrackederrors";

  private final String scopeStringCleanup;
  private final String scopeStringDelete;
  private final String scopeStringInit;
  private final String scopeStringInsert;
  private final String scopeStringRead;
  private final String scopeStringScan;
  private final String scopeStringUpdate;

  public DBWrapper(final DB db, final Tracer tracer) {
    this.db = db;
    measurements = Measurements.getMeasurements();
    this.tracer = tracer;
    final String simple = db.getClass().getSimpleName();
    scopeStringCleanup = simple + "#cleanup";
    scopeStringDelete = simple + "#delete";
    scopeStringInit = simple + "#init";
    scopeStringInsert = simple + "#insert";
    scopeStringRead = simple + "#read";
    scopeStringScan = simple + "#scan";
    scopeStringUpdate = simple + "#update";
  }

  /**
   * Set the properties for this DB.
   */
  public void setProperties(Properties p) {
    db.setProperties(p);
  }

  /**
   * Get the set of properties for this DB.
   */
  public Properties getProperties() {
    return db.getProperties();
  }

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
    try (final TraceScope span = tracer.newScope(scopeStringInit)) {
      db.init();

      this.reportLatencyForEachError = Boolean.parseBoolean(getProperties().
          getProperty(REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY,
              REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY_DEFAULT));

      if (!reportLatencyForEachError) {
        String latencyTrackedErrorsProperty = getProperties().getProperty(LATENCY_TRACKED_ERRORS_PROPERTY, null);
        if (latencyTrackedErrorsProperty != null) {
          this.latencyTrackedErrors = new HashSet<String>(Arrays.asList(
              latencyTrackedErrorsProperty.split(",")));
        }
      }

      System.err.println("DBWrapper: report latency for each error is " +
          this.reportLatencyForEachError + " and specific error codes to track" +
          " for latency are: " + this.latencyTrackedErrors.toString());
    }
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void cleanup() throws DBException {
    try (final TraceScope span = tracer.newScope(scopeStringCleanup)) {
      long ist = measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      db.cleanup();
      long en = System.nanoTime();
      measure("CLEANUP", Status.OK, ist, st, en);
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result
   * will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return The result of the operation.
   */
  public Status read(String table, String key, Set<String> fields,
                     HashMap<String, ByteIterator> result) {
    try (final TraceScope span = tracer.newScope(scopeStringRead)) {
      long ist = measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      Status res = db.read(table, key, fields, result);
      long en = System.nanoTime();
      measure("READ", res, ist, st, en);
      measurements.reportStatus("READ", res);
      return res;
    }
  }

  /**
   * Perform a range scan for a set of records in the database.
   * Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return The result of the operation.
   */
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try (final TraceScope span = tracer.newScope(scopeStringScan)) {
      long ist = measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      Status res = db.scan(table, startkey, recordcount, fields, result);
      long en = System.nanoTime();
      measure("SCAN", res, ist, st, en);
      measurements.reportStatus("SCAN", res);
      //System.out.println("Result="+result.size());
      HashMap<String, ArrayList<Double>> value = new HashMap<>();

      for(int i = 0; i < result.size(); i++) {

        String hashVal = result.get(i).get("field0").toString();
        String name = hashVal.split(":")[0];
        double val =  Double.valueOf(hashVal.split(":")[2]).doubleValue();

        if(value.containsKey(name)){
          ArrayList<Double> list = value.get(name);
          list.add(val);
        }else{
          ArrayList<Double> list = new ArrayList<>();
          list.add(val);
          value.put(name, list);
        }
        Iterator<String> it = value.keySet().iterator();
        Logger log = Logger.getGlobal() ;
        while(it.hasNext()){
          String iotKey = it.next();
          List<Double> l = value.get(iotKey);
          double sum = 0;
          for(int k=0; k<l.size();k++){
            sum = sum + l.get(k);
          }
          double avgVal = sum/l.size();
          System.out.println("Avg Value for "+iotKey+"="+avgVal);
//          //log.info("Avg Value for "+iotKey+"="+avgVal);
          it.remove();
        }
      }
      return res;
    }
  }

  public Status scan(String table, String key, String client, String timestamp,
                     Set<String> fields,long runStartTime, Vector<HashMap<String, ByteIterator>> result1,Vector<HashMap<String, ByteIterator>> result2) {
    try (final TraceScope span = tracer.newScope(scopeStringScan)) {
      long ist = measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      Status res = db.scan(table, key, client, timestamp, fields, runStartTime, result1, result2);
      //System.out.println("Results from scan"+res);
      //System.out.println("Result Size for Query1 = "+result1.size());
      //System.out.println("Result Size for Query2 = "+result2.size());
      
      //2020.10.07 TTA: Scan row count 0 return
      if(result1.size()==0 && result2.size()==0)
      {
        measurements.measureResultCount("SCAN",1,1);
      }
      else if(result1.size()==0)
      {
        measurements.measureResultCount("SCAN",1,0);
      }
      else if(result2.size()==0)
      {
        measurements.measureResultCount("SCAN",0,1);
      }

      long en = System.nanoTime();
      measure("SCAN", res, ist, st, en);
      measurements.reportStatus("SCAN", res);
     // System.out.println("Result="+result1.size());
     // System.out.println("Result2="+result2.size());

      HashMap<String, ArrayList<Double>> value = new HashMap<>();
      double val = 0;
      for(int i = 0; i < result1.size(); i++) {

        String hashVal = result1.get(i).get("field0").toString();
        //String name = hashVal.split(":")[0];

         val =  val + Double.valueOf(hashVal.split(":")[2]).doubleValue();
//        if(value.containsKey(key)){
//          ArrayList<Double> list = value.get(key);
//          list.add(val);
//        }else{
//          ArrayList<Double> list = new ArrayList<>();
//          list.add(val);
//          value.put(key, list);
//        }
//        if(value.containsKey(name)){
//          ArrayList<Double> list = value.get(name);
//          list.add(val);
//        }else{
//          ArrayList<Double> list = new ArrayList<>();
//          list.add(val);
//          value.put(name, list);
//        }
//        Iterator<String> it = value.keySet().iterator();
//        Logger log = Logger.getGlobal() ;
//        double avgVal=0.0;
//        while(it.hasNext()){
//          String iotKey = it.next();
//          List<Double> l = value.get(iotKey);
//          double sum = 0;
//          for(int k=0; k<l.size();k++){
//            sum = sum + l.get(k);
//          }
//           avgVal = sum/l.size();
//
////          //log.info("Avg Value for "+iotKey+"="+avgVal);
//          it.remove();
//        }

        //System.out.println("Avg Value for "+key+"="+avgVal);
      }
      if(result1.size() > 0) {
        double avgVal = val / result1.size();
        System.out.println("Latest Time Interval :: Avg Value for " + key + "=" + avgVal);
      }else{
        System.err.println("Unable to get query results from database, please check the status of the table ");
        return res;
      }
      val = 0;
      for(int i = 0; i < result2.size(); i++) {

        String hashVal = result2.get(i).get("field0").toString();

        val =  val + Double.valueOf(hashVal.split(":")[2]).doubleValue();

      }
      if( val > 0) {
        double avgVal2 = val / result2.size();
        System.out.println("30 Min Window Time Interval :: Avg Value for " + key + "=" + avgVal2);
      }
      return res;
    }
  }
  private void measure(String op, Status result, long intendedStartTimeNanos,
                       long startTimeNanos, long endTimeNanos) {
    String measurementName = op;
    if (result == null || !result.isOk()) {
      if (this.reportLatencyForEachError ||
          this.latencyTrackedErrors.contains(result.getName())) {
        measurementName = op + "-" + result.getName();
      } else {
        measurementName = op + "-FAILED";
      }
    }
    measurements.measure(measurementName,
        (int) ((endTimeNanos - startTimeNanos) / 1000));
    measurements.measureIntended(measurementName,
        (int) ((endTimeNanos - intendedStartTimeNanos) / 1000));
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key, overwriting any existing values with the same field name.
   *
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return The result of the operation.
   */
  public Status update(String table, String key,
                       HashMap<String, ByteIterator> values) {
    try (final TraceScope span = tracer.newScope(scopeStringUpdate)) {
      long ist = measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      Status res = db.update(table, key, values);
      long en = System.nanoTime();
      measure("UPDATE", res, ist, st, en);
      measurements.reportStatus("UPDATE", res);
      return res;
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified
   * record key.
   *
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return The result of the operation.
   */
  public Status insert(String table, String key,
                       HashMap<String, ByteIterator> values) {
    try (final TraceScope span = tracer.newScope(scopeStringInsert)) {
      long ist = measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      Status res = db.insert(table, key, values);
      long en = System.nanoTime();
      measure("INSERT", res, ist, st, en);
      measurements.reportStatus("INSERT", res);
      return res;
    }
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return The result of the operation.
   */
  public Status delete(String table, String key) {
    try (final TraceScope span = tracer.newScope(scopeStringDelete)) {
      long ist = measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      Status res = db.delete(table, key);
      long en = System.nanoTime();
      measure("DELETE", res, ist, st, en);
      measurements.reportStatus("DELETE", res);
      return res;
    }
  }
}
