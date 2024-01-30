/**
 * Copyright (c) 2010 Yahoo! Inc., Copyright (c) 2016 YCSB contributors. All rights reserved.
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

package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.*;
import com.yahoo.ycsb.measurements.Measurements;

import java.io.IOException;
import java.math.RoundingMode;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD operations. The
 * relative proportion of different kinds of operations, and other properties of the workload,
 * are controlled by parameters specified at runtime.
 * <p>
 * Properties to control the client:
 * <UL>
 * <LI><b>fieldcount</b>: the number of fields in a record (default: 10)
 * <LI><b>fieldlength</b>: the size of each field (default: 100)
 * <LI><b>readallfields</b>: should reads read all fields (true) or just one (false) (default: true)
 * <LI><b>writeallfields</b>: should updates and read/modify/writes update all fields (true) or just
 * one (false) (default: false)
 * <LI><b>readproportion</b>: what proportion of operations should be reads (default: 0.95)
 * <LI><b>updateproportion</b>: what proportion of operations should be updates (default: 0.05)
 * <LI><b>insertproportion</b>: what proportion of operations should be inserts (default: 0)
 * <LI><b>scanproportion</b>: what proportion of operations should be scans (default: 0)
 * <LI><b>readmodifywriteproportion</b>: what proportion of operations should be read a record,
 * modify it, write it back (default: 0)
 * <LI><b>requestdistribution</b>: what distribution should be used to select the records to operate
 * on - uniform, zipfian, hotspot, sequential, exponential or latest (default: uniform)
 * <LI><b>maxscanlength</b>: for scans, what is the maximum number of records to scan (default: 1000)
 * <LI><b>scanlengthdistribution</b>: for scans, what distribution should be used to choose the
 * number of records to scan, for each scan, between 1 and maxscanlength (default: uniform)
 * <LI><b>insertstart</b>: for parallel loads and runs, defines the starting record for this
 * YCSB instance (default: 0)
 * <LI><b>insertcount</b>: for parallel loads and runs, defines the number of records for this
 * YCSB instance (default: recordcount)
 * <LI><b>zeropadding</b>: for generating a record sequence compatible with string sort order by
 * 0 padding the record number. Controls the number of 0s to use for padding. (default: 1)
 * For example for row 5, with zeropadding=1 you get 'user5' key and with zeropading=8 you get
 * 'user00000005' key. In order to see its impact, zeropadding needs to be bigger than number of
 * digits in the record number.
 * <LI><b>insertorder</b>: should records be inserted in order by key ("ordered"), or in hashed
 * order ("hashed") (default: hashed)
 * </ul>
 */
public class CoreWorkload extends Workload {
  /**
   * The name of the database table to run queries against.
   */
  public static final String TABLENAME_PROPERTY = "table";

  /**
   * The default name of the database table to run queries against.
   */
  public static final String TABLENAME_PROPERTY_DEFAULT = "usertable";

  protected String table;

  /**
   * The name of the property for the number of fields in a record.
   */
  public static final String FIELD_COUNT_PROPERTY = "fieldcount";

  /**
   * Default number of fields in a record.
   */
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "1";


  protected String client;

  protected int nodeno;

  protected int instanceno;

  protected String runType;

  long runStartTime;

  public static final String DEFAULT_RUN_TYPE="run";

  public static final String DEFAULT_CLIENT_NAME="client1";

  protected int fieldcount;
   private String[] prekeys = {
           "cent_9_Humidity",
           "side_8_Humidity",
           "side_7_Humidity",
           "ang_30_Humidity",
           "ang_45_Humidity",
           "ang_60_Humidity",
           "ang_90_Humidity",
           "bef_1195_Humidity",
           "aft_1120_Humidity",
           "mid_1125_Humidity",
           "cor_4_Humidity",
           "cor_1_Humidity",
           "cor_5_Humidity",
           "cent_9_Power",
           "side_8_Power",
           "side_7_Power",
           "ang_30_Power",
           "ang_45_Power",
           "ang_60_Power",
           "ang_90_Power",
           "bef_1195_Power",
           "aft_1120_Power",
           "mid_1125_Power",
           "cor_4_Power",
           "cor_1_Power",
           "cor_5_Power",
           "cent_9_Pressure",
           "side_8_Pressure",
           "side_7_Pressure",
           "ang_30_Pressure",
           "ang_45_Pressure",
           "ang_60_Pressure",
           "ang_90_Pressure",
           "bef_1195_Pressure",
           "aft_1120_Pressure",
           "mid_1125_Pressure",
           "cor_4_Pressure",
           "cor_1_Pressure",
           "cor_5_Pressure",
           "cent_9_Flow",
           "side_8_Flow",
           "side_7_Flow",
           "ang_30_Flow",
           "ang_45_Flow",
           "ang_60_Flow",
           "ang_90_Flow",
           "bef_1195_Flow",
           "aft_1120_Flow",
           "mid_1125_Flow",
           "cor_4_Flow",
           "cor_1_Flow",
           "cor_5_Flow",
           "cent_9_Level",
           "side_8_Level",
           "side_7_Level",
           "ang_30_Level",
           "ang_45_Level",
           "ang_60_Level",
           "ang_90_Level",
           "bef_1195_Level",
           "aft_1120_Level",
           "mid_1125_Level",
           "cor_4_Level",
           "cor_1_Level",
           "cor_5_Level",
           "cent_9_Temperature",
           "side_8_Temperature",
           "side_7_Temperature",
           "ang_30_Temperature",
           "ang_45_Temperature",
           "ang_60_Temperature",
           "ang_90_Temperature",
           "bef_1195_Temperature",
           "aft_1120_Temperature",
           "mid_1125_Temperature",
           "cor_4_Temperature",
           "cor_1_Temperature",
           "cor_5_Temperature",
           "cent_9_vibration",
           "side_8_vibration",
           "side_7_vibration",
           "ang_30_vibration",
           "ang_45_vibration",
           "ang_60_vibration",
           "ang_90_vibration",
           "bef_1195_vibration",
           "aft_1120_vibration",
           "mid_1125_vibration",
           "cor_4_vibration",
           "cor_1_vibration",
           "cor_5_vibration",
           "cent_9_tilt",
           "side_8_tilt",
           "side_7_tilt",
           "ang_30_tilt",
           "ang_45_tilt",
           "ang_60_tilt",
           "ang_90_tilt",
           "bef_1195_tilt",
           "aft_1120_tilt",
           "mid_1125_tilt",
           "cor_4_tilt",
           "cor_1_tilt",
           "cor_5_tilt",
           "cent_9_level",
           "side_8_level",
           "side_7_level",
           "ang_30_level",
           "ang_45_level",
           "ang_60_level",
           "ang_90_level",
           "bef_1195_level",
           "aft_1120_level",
           "mid_1125_level",
           "cor_4_level",
           "cor_1_level",
           "cor_5_level",
           "cent_9_level_vibrating",
           "side_8_level_vibrating",
           "side_7_level_vibrating",
           "ang_30_level_vibrating",
           "ang_45_level_vibrating",
           "ang_60_level_vibrating",
           "ang_90_level_vibrating",
           "bef_1195_level_vibrating",
           "aft_1120_level_vibrating",
           "mid_1125_level_vibrating",
           "cor_4_level_vibrating",
           "cor_1_level_vibrating",
           "cor_5_level_vibrating",
           "cent_9_level_rotating",
           "side_8_level_rotating",
           "side_7_level_rotating",
           "ang_30_level_rotating",
           "ang_45_level_rotating",
           "ang_60_level_rotating",
           "ang_90_level_rotating",
           "bef_1195_level_rotating",
                   "aft_1120_level_rotating",
                   "mid_1125_level_rotating",
                   "cor_4_level_rotating",
                   "cor_1_level_rotating",
                   "cor_5_level_rotating",
                   "cent_9_level_admittance",
                   "side_8_level_admittance",
                   "side_7_level_admittance",
                   "ang_30_level_admittance",
                   "ang_45_level_admittance",
                   "ang_60_level_admittance",
                   "ang_90_level_admittance",
                   "bef_1195_level_admittance",
                   "aft_1120_level_admittance",
                   "mid_1125_level_admittance",
                   "cor_4_level_admittance",
                   "cor_1_level_admittance",
                   "cor_5_level_admittance",
                   "cent_9_Pneumatic_level",
                   "side_8_Pneumatic_level",
                   "side_7_Pneumatic_level",
                   "ang_30_Pneumatic_level"
   };
//   private int totalinsertcount = 1_000_000_000;
   private long totalinsertcount = 4_000_000_000L;
   private int nodenum = 4;
   private int instancenum = 11;
   private int insertcount = (int) (totalinsertcount / nodenum / instancenum);
//   private int[] choseninstances = {42, 30, 39, 29, 44, 5, 34, 24, 16}; // 1 1 3 4
   private int[] choseninstances = {42, 40, 39, 38, 44, 43, 34, 37, 41}; // 0 0 0 9
//   private int[] choseninstances = {1, 2, 12, 13, 23, 24, 34, 35, 36}; // 2 2 2 3

  private List<String> fieldnames;

  /**
   * The name of the property for the field length distribution. Options are "uniform", "zipfian"
   * (favouring short records), "constant", and "histogram".
   * <p>
   * If "uniform", "zipfian" or "constant", the maximum field length will be that specified by the
   * fieldlength property. If "histogram", then the histogram will be read from the filename
   * specified in the "fieldlengthhistogram" property.
   */
  public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY = "fieldlengthdistribution";

  public static final String CLIENT_NAME="client";

  public static final String RUN_TYPE="runtype";

  /**
   * The default field length distribution.
   */
  public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";

  /**
   * The name of the property for the length of a field in bytes.
   */
  public static final String FIELD_LENGTH_PROPERTY = "fieldlength";

  /**
   * The default maximum length of a field in bytes.
   */
  public static final String FIELD_LENGTH_PROPERTY_DEFAULT = "1000";

  /**
   * The name of a property that specifies the filename containing the field length histogram (only
   * used if fieldlengthdistribution is "histogram").
   */
  public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY = "fieldlengthhistogram";

  /**
   * The default filename containing a field length histogram.
   */
  public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";

  /**
   * Generator object that produces field lengths.  The value of this depends on the properties that
   * start with "FIELD_LENGTH_".
   */
  protected NumberGenerator fieldlengthgenerator;

  /**
   * The name of the property for deciding whether to read one field (false) or all fields (true) of
   * a record.
   */
  public static final String READ_ALL_FIELDS_PROPERTY = "readallfields";

  /**
   * The default value for the readallfields property.
   */
  public static final String READ_ALL_FIELDS_PROPERTY_DEFAULT = "true";

  protected boolean readallfields;

  /**
   * The name of the property for deciding whether to write one field (false) or all fields (true)
   * of a record.
   */
  public static final String WRITE_ALL_FIELDS_PROPERTY = "writeallfields";

  /**
   * The default value for the writeallfields property.
   */
  public static final String WRITE_ALL_FIELDS_PROPERTY_DEFAULT = "false";

  protected boolean writeallfields;

  /**
   * The name of the property for deciding whether to check all returned
   * data against the formation template to ensure data integrity.
   */
  public static final String DATA_INTEGRITY_PROPERTY = "dataintegrity";

  /**
   * The default value for the dataintegrity property.
   */
  public static final String DATA_INTEGRITY_PROPERTY_DEFAULT = "true";

  /**
   * Set to true if want to check correctness of reads. Must also
   * be set to true during loading phase to function.
   */
  private boolean dataintegrity;

  /**
   * The name of the property for the proportion of transactions that are reads.
   */
  public static final String READ_PROPORTION_PROPERTY = "readproportion";

  /**
   * The default proportion of transactions that are reads.
   */
  public static final String READ_PROPORTION_PROPERTY_DEFAULT = "0.00";

  /**
   * The name of the property for the proportion of transactions that are updates.
   */
  public static final String UPDATE_PROPORTION_PROPERTY = "updateproportion";

  /**
   * The default proportion of transactions that are updates.
   */
  public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT = "0.05";

  /**
   * The name of the property for the proportion of transactions that are inserts.
   */
  public static final String INSERT_PROPORTION_PROPERTY = "insertproportion";

  /**
   * The default proportion of transactions that are inserts.
   */
  public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "1.0";

  /**
   * The name of the property for the proportion of transactions that are scans.
   */
  public static final String SCAN_PROPORTION_PROPERTY = "scanproportion";

  /**
   * The default proportion of transactions that are scans.
   */
  public static final String SCAN_PROPORTION_PROPERTY_DEFAULT = "0.00005";

  /**
   * The name of the property for the proportion of transactions that are read-modify-write.
   */
  public static final String READMODIFYWRITE_PROPORTION_PROPERTY = "readmodifywriteproportion";

  /**
   * The default proportion of transactions that are scans.
   */
  public static final String READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT = "0.0";

  /**
   * The name of the property for the the distribution of requests across the keyspace. Options are
   * "uniform", "zipfian" and "latest"
   */
  public static final String REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";

  /**
   * The default distribution of requests across the keyspace.
   */
  public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

  /**
   * The name of the property for adding zero padding to record numbers in order to match
   * string sort order. Controls the number of 0s to left pad with.
   */
  public static final String ZERO_PADDING_PROPERTY = "zeropadding";

  /**
   * The default zero padding value. Matches integer sort order
   */
  public static final String ZERO_PADDING_PROPERTY_DEFAULT = "1";


  /**
   * The name of the property for the max scan length (number of records).
   */
  public static final String MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";

  /**
   * The default max scan length.
   */
  public static final String MAX_SCAN_LENGTH_PROPERTY_DEFAULT = "100";

  /**
   * The name of the property for the scan length distribution. Options are "uniform" and "zipfian"
   * (favoring short scans)
   */
  public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY = "scanlengthdistribution";

  /**
   * The default max scan length.
   */
  public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

  /**
   * The name of the property for the order to insert records. Options are "ordered" or "hashed"
   */
  public static final String INSERT_ORDER_PROPERTY = "insertorder";

  /**
   * Default insert order.
   */
  public static final String INSERT_ORDER_PROPERTY_DEFAULT = "hashed";

  /**
   * Percentage data items that constitute the hot set.
   */
  public static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";

  /**
   * Default value of the size of the hot set.
   */
  public static final String HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";

  /**
   * Percentage operations that access the hot set.
   */
  public static final String HOTSPOT_OPN_FRACTION = "hotspotopnfraction";

  /**
   * Default value of the percentage operations accessing the hot set.
   */
  public static final String HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";

  /**
   * How many times to retry when insertion of a single item to a DB fails.
   */
  public static final String INSERTION_RETRY_LIMIT = "core_workload_insertion_retry_limit";
  public static final String INSERTION_RETRY_LIMIT_DEFAULT = "0";

  /**
   * On average, how long to wait between the retries, in seconds.
   */
  public static final String INSERTION_RETRY_INTERVAL = "core_workload_insertion_retry_interval";
  public static final String INSERTION_RETRY_INTERVAL_DEFAULT = "3";

  protected NumberGenerator keysequence;
  protected DiscreteGenerator operationchooser;
  protected NumberGenerator keychooser;
  protected NumberGenerator writeKeyChooser = new UniformIntegerGenerator(0, prekeys.length - 1);
  protected NumberGenerator readKeyChooser = new UniformIntegerGenerator(0, prekeys.length - 1);
  protected NumberGenerator fieldchooser;
  protected AcknowledgedCounterGenerator transactioninsertkeysequence;
  protected NumberGenerator scanlength;
  protected boolean orderedinserts;
  protected long recordcount;
  protected int zeropadding;
  protected int insertionRetryLimit;
  protected int insertionRetryInterval;
  protected UnixEpochTimestampGenerator tt;

  private Measurements measurements = Measurements.getMeasurements();

  protected static NumberGenerator getFieldLengthGenerator(Properties p) throws WorkloadException {
    NumberGenerator fieldlengthgenerator;
    String fieldlengthdistribution = p.getProperty(
        FIELD_LENGTH_DISTRIBUTION_PROPERTY, FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
    int fieldlength =
        Integer.parseInt(p.getProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_PROPERTY_DEFAULT));
    String fieldlengthhistogram = p.getProperty(
        FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY, FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT);
    if (fieldlengthdistribution.compareTo("constant") == 0) {
      fieldlengthgenerator = new ConstantIntegerGenerator(fieldlength);
    } else if (fieldlengthdistribution.compareTo("uniform") == 0) {
      fieldlengthgenerator = new UniformIntegerGenerator(1, fieldlength);
    } else if (fieldlengthdistribution.compareTo("zipfian") == 0) {
      fieldlengthgenerator = new ZipfianGenerator(1, fieldlength);
    } else if (fieldlengthdistribution.compareTo("histogram") == 0) {
      try {
        fieldlengthgenerator = new HistogramGenerator(fieldlengthhistogram);
      } catch (IOException e) {
        throw new WorkloadException(
            "Couldn't read field length histogram file: " + fieldlengthhistogram, e);
      }
    } else {
      throw new WorkloadException(
          "Unknown field length distribution \"" + fieldlengthdistribution + "\"");
    }
    return fieldlengthgenerator;
  }

  /**
   * Initialize the scenario.
   * Called once, in the main client thread, before any operations are started.
   */
  @Override
  public void init(Properties p) throws WorkloadException {
    table = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);
     client = p.getProperty(CLIENT_NAME, DEFAULT_CLIENT_NAME);
     nodeno = Integer.parseInt(client.substring(3, 4));
     instanceno = Integer.parseInt(client.substring(4));
      runType = p.getProperty(RUN_TYPE,DEFAULT_RUN_TYPE);
     //System.out.println("CLIENT NAME="+client);
    System.out.println("Run Type = "+runType);

    fieldcount =
        Integer.parseInt(p.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));
    fieldnames = new ArrayList<>();
    for (int i = 0; i < fieldcount; i++) {
      fieldnames.add("field" + i);
    }
    fieldlengthgenerator = CoreWorkload.getFieldLengthGenerator(p);

    recordcount =
        Long.parseLong(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
    if (recordcount == 0) {
      recordcount = Long.MAX_VALUE;
    }

    //System.out.println("Record Count = "+recordcount);
    String requestdistrib =
        p.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
    int maxscanlength =
        Integer.parseInt(p.getProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_PROPERTY_DEFAULT));
    String scanlengthdistrib =
        p.getProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY, SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);

    long insertstart =
        Long.parseLong(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));
    long insertcount =
        Long.parseLong(p.getProperty(INSERT_COUNT_PROPERTY, String.valueOf(recordcount - insertstart)));
    // Confirm valid values for insertstart and insertcount in relation to recordcount
    if (recordcount < (insertstart + insertcount)) {
      System.err.println("Invalid combination of insertstart, insertcount and recordcount.");
      System.err.println("recordcount must be bigger than insertstart + insertcount.");
      System.exit(-1);
    }
    zeropadding =
        Integer.parseInt(p.getProperty(ZERO_PADDING_PROPERTY, ZERO_PADDING_PROPERTY_DEFAULT));

    readallfields = Boolean.parseBoolean(
        p.getProperty(READ_ALL_FIELDS_PROPERTY, READ_ALL_FIELDS_PROPERTY_DEFAULT));
    writeallfields = Boolean.parseBoolean(
        p.getProperty(WRITE_ALL_FIELDS_PROPERTY, WRITE_ALL_FIELDS_PROPERTY_DEFAULT));

    dataintegrity = Boolean.parseBoolean(
        p.getProperty(DATA_INTEGRITY_PROPERTY, DATA_INTEGRITY_PROPERTY_DEFAULT));
    // Confirm that fieldlengthgenerator returns a constant if data
    // integrity check requested.
    if (dataintegrity && !(p.getProperty(
        FIELD_LENGTH_DISTRIBUTION_PROPERTY,
        FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT)).equals("constant")) {
      System.err.println("Must have constant field size to check data integrity.");
      System.exit(-1);
    }

    if (p.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed") == 0) {
      orderedinserts = false;
    } else if (requestdistrib.compareTo("exponential") == 0) {
      double percentile = Double.parseDouble(p.getProperty(
          ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY,
          ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
      double frac = Double.parseDouble(p.getProperty(
          ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY,
          ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
      keychooser = new ExponentialGenerator(percentile, recordcount * frac);
    } else {
      orderedinserts = true;
    }

    keysequence = new CounterGenerator(insertstart);
    operationchooser = createOperationGenerator(p);

    transactioninsertkeysequence = new AcknowledgedCounterGenerator(recordcount);
    tt = new UnixEpochTimestampGenerator(100,TimeUnit.MILLISECONDS, System.currentTimeMillis());


    if (requestdistrib.compareTo("uniform") == 0) {
        keychooser = new UniformLongGenerator(insertstart, insertstart + insertcount - 1);
    /*
     * FROM CODE REVIEW:
     *
     * As these generator functions are not used in IoT, it is safe to comment them out.
     * This saves us the work of building long versions of all the generator functions.
     * We need to add a comment to this effect.
     *
      } else if (requestdistrib.compareTo("sequential") == 0) {
      keychooser = new SequentialGenerator(insertstart, insertstart + insertcount - 1);
    } else if (requestdistrib.compareTo("zipfian") == 0) {
      // it does this by generating a random "next key" in part by taking the modulus over the
      // number of keys.
      // If the number of keys changes, this would shift the modulus, and we don't want that to
      // change which keys are popular so we'll actually construct the scrambled zipfian generator
      // with a keyspace that is larger than exists at the beginning of the test. that is, we'll predict
      // the number of inserts, and tell the scrambled zipfian generator the number of existing keys
      // plus the number of predicted keys as the total keyspace. then, if the generator picks a key
      // that hasn't been inserted yet, will just ignore it and pick another key. this way, the size of
      // the keyspace doesn't change from the perspective of the scrambled zipfian generator
      final double insertproportion = Double.parseDouble(
          p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
      int opcount = Integer.parseInt(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
      int expectednewkeys = (int) ((opcount) * insertproportion * 2.0); // 2 is fudge factor

      keychooser = new ScrambledZipfianGenerator(insertstart, insertstart + insertcount + expectednewkeys);
    } else if (requestdistrib.compareTo("latest") == 0) {
      keychooser = new SkewedLatestGenerator(transactioninsertkeysequence);
    } else if (requestdistrib.equals("hotspot")) {
      double hotsetfraction =
          Double.parseDouble(p.getProperty(HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
      double hotopnfraction =
          Double.parseDouble(p.getProperty(HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
      keychooser = new HotspotIntegerGenerator(insertstart, insertstart + insertcount - 1,
          hotsetfraction, hotopnfraction);
    */} else {
      throw new WorkloadException("Unknown request distribution \"" + requestdistrib + "\"");
    }

    fieldchooser = new UniformIntegerGenerator(0, fieldcount - 1);

    if (scanlengthdistrib.compareTo("uniform") == 0) {
      scanlength = new UniformIntegerGenerator(1, maxscanlength);
    } else if (scanlengthdistrib.compareTo("zipfian") == 0) {
      scanlength = new ZipfianGenerator(1, maxscanlength);
    } else {
      throw new WorkloadException(
          "Distribution \"" + scanlengthdistrib + "\" not allowed for scan length");
    }

    insertionRetryLimit = Integer.parseInt(p.getProperty(
        INSERTION_RETRY_LIMIT, INSERTION_RETRY_LIMIT_DEFAULT));
    insertionRetryInterval = Integer.parseInt(p.getProperty(
        INSERTION_RETRY_INTERVAL, INSERTION_RETRY_INTERVAL_DEFAULT));

    runStartTime = System.currentTimeMillis();
  }

  protected String buildKeyName(long keynum) {

    int index = writeKeyChooser.nextValue().intValue();
    String prekey = prekeys[index];
    String key =  client + ":" + prekey +  ":" + keynum;
   
    try {
      this.wait(1000);
    }catch (Exception e){}
    return  key;
  }

    protected String buildKeyNameForRead(long keynum) {

        int index = readKeyChooser.nextValue().intValue();
        String prekey = prekeys[index];
        // Read the keys that are than 5s old
//        long t = tt.lastValue() - 5000;

        long t = Math.max(0, keychooser.lastValue().intValue() - 5000);
        return   client + ":" + prekey +  ":" + t;
    }

  /**
   * Builds a value for a randomly chosen field.
   */
  private HashMap<String, ByteIterator> buildSingleValue(String key) {
    HashMap<String, ByteIterator> value = new HashMap<>();

    String fieldkey = fieldnames.get(fieldchooser.nextValue().intValue());
    ByteIterator data;
    if (dataintegrity) {
      data = new StringByteIterator(buildDeterministicValue(key, fieldkey));
    } else {
      // fill with random data
      data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
    }
    value.put(fieldkey, data);

    return value;
  }

  /**
   * Builds values for all fields.
   */
  private HashMap<String, ByteIterator> buildValues(String key) {
    HashMap<String, ByteIterator> values = new HashMap<>();
    for (String fieldkey : fieldnames) {
      ByteIterator data;
      if (dataintegrity) {
        data = new StringByteIterator(buildDeterministicValue(key, fieldkey));
      } else {
        // fill with random data
        data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
      }
      values.put(fieldkey, data);
    }
    return values;
  }

  /**
   * Build a deterministic value given the key information.
   */
  private String buildDeterministicValue(String key, String fieldkey) {
    int size = fieldlengthgenerator.nextValue().intValue();

   // System.out.println("size = "+size);
      String iotParameter = null;
      if(key.contains(":")) {
          iotParameter = key.split(":")[1];
      }
      else
          iotParameter = key;
    Random r = new Random();
    StringBuilder sb = new StringBuilder(size);
    sb.append(iotParameter);
    BigDecimal val = BigDecimal.valueOf(r.nextDouble()).setScale(4, RoundingMode.HALF_UP);
    while (sb.length() < size) {
      sb.append(':');
        sb.append(iotParameter);
        sb.append('_');
        sb.append("value");
        sb.append(':');
        sb.append(val);
        sb.append(':');
        sb.append("timestamp");
        sb.append(':');
        sb.append(System.currentTimeMillis());
        sb.append(":");
        sb.append(sb.toString().hashCode());
    }
    sb.setLength(size);
    //System.out.println("SB="+sb.toString());
    return sb.toString();
  }

  /**
   * Do one insert operation. Because it will be called concurrently from multiple client threads,
   * this function must be thread safe. However, avoid synchronized, or the threads will block waiting
   * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than DB operations.
   */
  @Override
  public boolean doInsert(DB db, Object threadstate) {
    int keynum = keysequence.nextValue().intValue();
    boolean ischosen = false;
    int no = (nodeno - 1) * instancenum + instanceno;
    for (int instance : choseninstances) {
      if (instance == no) {
        ischosen = true;
        break;
      }
    }
    if (!ischosen && keynum >= insertcount / 16) {
      return true;
    }
//    if (keynum >= insertcount / 16 && (nodeno != 1 || instanceno > 9)) {
//      return true;
//    }
    String dbkey = buildKeyName(keynum);
    HashMap<String, ByteIterator> values = buildValues(dbkey);

    Status status;
    int numOfRetries = 0;
    do {
      //System.out.println("DB Key ="+dbkey);
      status = db.insert(table, dbkey, values);
      if (null != status && status.isOk()) {
        break;
      }
      // Retry if configured. Without retrying, the load process will fail
      // even if one single insertion fails. User can optionally configure
      // an insertion retry limit (default is 0) to enable retry.
      if (++numOfRetries <= insertionRetryLimit) {
        System.err.println("Retrying insertion, retry count: " + numOfRetries);
        try {
          // Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
          int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4 * Math.random()));
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          break;
        }

      } else {
        System.err.println("Error inserting, not retrying any more. number of attempts: " + numOfRetries +
            "Insertion Retry Limit: " + insertionRetryLimit);
        break;

      }
    } while (true);

    return null != status && status.isOk();
  }

  /**
   * Do one transaction operation. Because it will be called concurrently from multiple client
   * threads, this function must be thread safe. However, avoid synchronized, or the threads will block waiting
   * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than DB operations.
   */
  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    String operation = operationchooser.nextString();
    if(operation == null) {
      return false;
    }

    switch (operation) {
    case "READ":
      doTransactionRead(db);
      break;
    case "UPDATE":
      doTransactionUpdate(db);
      break;
    case "INSERT":
      doTransactionInsert(db);
      break;
    case "SCAN":
      doTransactionScanWithFilter(db, runStartTime);
      break;
    default:
      doTransactionReadModifyWrite(db);
    }

    return true;
  }

  /**
   * Results are reported in the first three buckets of the histogram under
   * the label "VERIFY".
   * Bucket 0 means the expected data was returned.
   * Bucket 1 means incorrect data was returned.
   * Bucket 2 means null data was returned when some data was expected.
   */
  protected void verifyRow(String key, HashMap<String, ByteIterator> cells) {
    Status verifyStatus = Status.OK;
    long startTime = System.nanoTime();
    if (!cells.isEmpty()) {
      for (Map.Entry<String, ByteIterator> entry : cells.entrySet()) {
        if (!entry.getValue().toString().equals(buildDeterministicValue(key, entry.getKey()))) {
          verifyStatus = Status.UNEXPECTED_STATE;
          break;
        }
      }
    } else {
      // This assumes that null data is never valid
      verifyStatus = Status.ERROR;
    }
    long endTime = System.nanoTime();
    measurements.measure("VERIFY", (int) (endTime - startTime) / 1000);
    measurements.reportStatus("VERIFY", verifyStatus);
  }

  protected long nextKeynum() {
    long keynum;
    if (keychooser instanceof ExponentialGenerator) {
      do {
        keynum = transactioninsertkeysequence.lastValue() - keychooser.nextValue().longValue();
      } while (keynum < 0);
    } else {
      do {
        keynum = keychooser.nextValue().longValue();
      } while (keynum > transactioninsertkeysequence.lastValue());
    }
    return keynum;
  }

  public void doTransactionRead(DB db) {

      String startkeyname = buildKeyNameForRead(nextKeynum());
      //System.out.println("Start key name="+startkeyname);
      // choose a random scan length
      int len = scanlength.nextValue().intValue();

      HashSet<String> fields = null;

      if (!readallfields) {
          // read a random field
          String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

          fields = new HashSet<String>();
          fields.add(fieldname);
      }
             Vector <HashMap<String, ByteIterator>> results = new Vector <HashMap<String, ByteIterator>>();
      db.scan(table, startkeyname, len, fields, results);

  }

  public void doTransactionReadModifyWrite(DB db) {
    // choose a random key
    long keynum = nextKeynum();

    String keyname = buildKeyName(keynum);

    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

      fields = new HashSet<String>();
      fields.add(fieldname);
    }

    HashMap<String, ByteIterator> values;

    if (writeallfields) {
      // new data for all the fields
      values = buildValues(keyname);
    } else {
      // update a random field
      values = buildSingleValue(keyname);
    }

    // do the transaction

    HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();


    long ist = measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    db.read(table, keyname, fields, cells);

    db.update(table, keyname, values);

    long en = System.nanoTime();

    if (dataintegrity) {
      verifyRow(keyname, cells);
    }

    measurements.measure("READ-MODIFY-WRITE", (int) ((en - st) / 1000));
    measurements.measureIntended("READ-MODIFY-WRITE", (int) ((en - ist) / 1000));
  }

  public void doTransactionScan(DB db) {
    // choose a random key
    //int keynum = nextKeynum();

    String startkeyname = buildKeyNameForRead(nextKeynum());

    // choose a random scan length
    int len = scanlength.nextValue().intValue();

    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

      fields = new HashSet<String>();
      fields.add(fieldname);
    }


      Vector <HashMap<String, ByteIterator>> results = new Vector <HashMap<String, ByteIterator>>();
    long t1 = System.currentTimeMillis();
    db.scan(table, startkeyname, len, fields, results);
    long t2 = System.currentTimeMillis();
    //System.out.println(new java.util.Date(System.currentTimeMillis()) + " :: Total Query time ="+(t2-t1));

  }

  public void doTransactionScanWithFilter(DB db, long runStartTime){

    //int keynum = nextKeynum();
    //System.out.println("DB ="+db.getClass());
    long keynum = nextKeynum();
    
    String startKeyName = buildKeyNameForRead(keynum);
    String client = startKeyName.split(":")[0];
    String filter =    startKeyName.split(":")[1];
    String timestamp = startKeyName.split(":")[2];


    HashSet<String> fields = null;
    Vector <HashMap<String, ByteIterator>> results1 = new Vector <HashMap<String, ByteIterator>>();
    Vector <HashMap<String, ByteIterator>> results2 = new Vector <HashMap<String, ByteIterator>>();
//    long newRunStartTime = Long.parseLong(timestamp);
//    db.scan(table, filter, client, timestamp,fields, newRunStartTime > 1800000L ? 0L : newRunStartTime, results1, results2);
    db.scan(table, filter, client, timestamp,fields, runStartTime, results1, results2);
  }

  public void doTransactionUpdate(DB db) {
    // choose a random keyscan
    long keynum = nextKeynum();

    String keyname = buildKeyName(keynum);

    HashMap<String, ByteIterator> values;

    if (writeallfields) {
      // new data for all the fields
      values = buildValues(keyname);
    } else {
      // update a random field
      values = buildSingleValue(keyname);
    }

    db.update(table, keyname, values);
  }

  public void doTransactionInsert(DB db) {
//    long keynum = tt.nextValue();
    int keynum = keysequence.nextValue().intValue();
    boolean ischosen = false;
    int no = (nodeno - 1) * instancenum + instanceno;
    for (int instance : choseninstances) {
      if (instance == no) {
        ischosen = true;
        break;
      }
    }
    if (!ischosen && keynum >= insertcount / 16) {
      return;
    }

    try {

      String dbkey = buildKeyName(keynum);
      HashMap<String, ByteIterator> values = buildValues(dbkey);
      db.insert(table, dbkey, values);
    } finally {

      //transactioninsertkeysequence.acknowledge((int)keynum);
    }
  }

  /**
   * Creates a weighted discrete values with database operations for a workload to perform.
   * Weights/proportions are read from the properties list and defaults are used
   * when values are not configured.
   * Current operations are "READ", "UPDATE", "INSERT", "SCAN" and "READMODIFYWRITE".
   *
   * @param p The properties list to pull weights from.
   * @return A generator that can be used to determine the next operation to perform.
   * @throws IllegalArgumentException if the properties object was null.
   */
  protected static DiscreteGenerator createOperationGenerator(final Properties p) {
    if (p == null) {
      throw new IllegalArgumentException("Properties object cannot be null");
    }

    final double insertproportion = Double.parseDouble(
        p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
    final double scanproportion = Double.parseDouble(
        p.getProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_PROPERTY_DEFAULT));
    final DiscreteGenerator operationchooser = new DiscreteGenerator();

    if (insertproportion > 0) {
      //System.out.println("Insert Proportion"+insertproportion);
      operationchooser.addValue(insertproportion, "INSERT");
    }

    if (scanproportion > 0) {
      //System.out.println("Scan Proportion"+scanproportion);
      operationchooser.addValue(scanproportion, "SCAN");
    }
    return operationchooser;
  }
}
