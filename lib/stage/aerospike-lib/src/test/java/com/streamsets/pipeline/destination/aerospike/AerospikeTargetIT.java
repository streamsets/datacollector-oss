/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.destination.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.policy.QueryPolicy;
import com.google.common.collect.Lists;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.*;

import static org.junit.Assert.*;

public class AerospikeTargetIT {
  private static final int AEROSPIKE_PORT = 3000;

  private static final GenericContainer aerospikeServer = new GenericContainer("aerospike/aerospike-server:3.15.0.2").withExposedPorts(AEROSPIKE_PORT);
  private static AerospikeClient client;


  @BeforeClass
  public static void waitTillConnected() throws InterruptedException {
    aerospikeServer.start();
    int retries = 0;
    synchronized (aerospikeServer) {
      while (retries < 50) {
        if (client == null || !client.isConnected()) {
          try {
            client = new AerospikeClient(aerospikeServer.getContainerIpAddress(), aerospikeServer.getMappedPort(AEROSPIKE_PORT));
            return;
          } catch (AerospikeException e) {
            Thread.sleep(100);
          }
        }
        retries++;
      }
    }
    throw new RuntimeException("Aerospike server docker not available");
  }

  private static com.aerospike.client.Record getRecord(String namespace, String set, String key) throws InterruptedException, AerospikeException {
    synchronized (aerospikeServer) {
      return client.get(new QueryPolicy(), new Key(namespace, set, key));
    }
  }

  @AfterClass
  public static void tearDown() {
    if (client != null) {
      client.close();
    }
  }

  private static AerospikeDTarget getDefaultConfig() {
    AerospikeDTarget config = new AerospikeDTarget();
    config.aerospikeBeanConfig = new AerospikeBeanConfig();
    config.aerospikeBeanConfig.maxRetries = 5;
    config.aerospikeBeanConfig.connectionString = Arrays.asList(aerospikeServer.getContainerIpAddress() + ":" + aerospikeServer.getMappedPort(AEROSPIKE_PORT));
    config.binConfigsEL = new LinkedList<BinConfig>();
    config.binConfigsEL.add(new BinConfig("${record:value('/bName1')}", "${record:value('/bValue1')}", DataType.STRING));
    config.namespaceEL = "${record:value('/namespace')}";
    config.keyEL = "${record:value('/key')}";
    config.setEL = "${record:value('/set')}";
    return config;
  }

  private static Record getTestRecord(String namespace, String set, String key, String binName) {
    Record record = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("namespace", Field.create(namespace));
    fields.put("set", Field.create(set));
    fields.put("key", Field.create(key));

    // record string type
    for (int i = 1; i <= 4; i++) {
      fields.put("bName" + i, Field.create(binName + i));
      fields.put("bValue" + i, Field.create(i));
    }
    record.set(Field.create(fields));
    return record;
  }

  @Test
  public void testInvalidUri() throws Exception {
    AerospikeDTarget config = getDefaultConfig();
    config.aerospikeBeanConfig.connectionString = Arrays.asList("xxx:abc");
    Target target = config.createTarget();
    TargetRunner runner = new TargetRunner.Builder(AerospikeDTarget.class, target).setOnRecordError(OnRecordError.TO_ERROR).build();
    List<Target.ConfigIssue> issues = runner.runValidateConfigs();
    assertTrue("Issues contains invalid uri error", issues.stream().anyMatch(configIssue -> configIssue.toString().contains("AEROSPIKE_02")));
  }

  @Test
  public void testUnreachableCluster() throws Exception {
    AerospikeDTarget config = getDefaultConfig();
    config.aerospikeBeanConfig.connectionString = Arrays.asList("xxx:123");
    Target target = config.createTarget();
    TargetRunner runner = new TargetRunner.Builder(AerospikeDTarget.class, target).setOnRecordError(OnRecordError.TO_ERROR).build();
    List<Target.ConfigIssue> issues = runner.runValidateConfigs();
    assertTrue("Issues contains unable to connect error", issues.stream().anyMatch(configIssue -> configIssue.toString().contains("AEROSPIKE_03")));
  }

  @Test
  public void testEmptyFields() throws Exception {
    AerospikeDTarget config = getDefaultConfig();
    config.setEL = "";
    config.binConfigsEL.add(new BinConfig("", "", DataType.STRING));
    config.binConfigsEL.add(new BinConfig("nonEmpty", "nonEmpty", DataType.STRING));

    Target target = config.createTarget();
    TargetRunner runner = new TargetRunner.Builder(AerospikeDTarget.class, target).setOnRecordError(OnRecordError.TO_ERROR).build();
    List<Target.ConfigIssue> issues = runner.runValidateConfigs();
    assertTrue("Issues contain config required error for bin name", issues.stream().anyMatch(
        configIssue ->
            configIssue.toString().contains("AEROSPIKE_01") &&
                configIssue.toString().contains("binName")
    ));
    assertTrue("Issues contain config required error for bin value", issues.stream().anyMatch(
        configIssue ->
            configIssue.toString().contains("AEROSPIKE_01") &&
                configIssue.toString().contains("binValue")
    ));
    assertFalse("Issues do not contain config required error for set", issues.stream().anyMatch(
        configIssue ->
            configIssue.toString().contains("AEROSPIKE_01") &&
                configIssue.toString().contains("setEL")
    ));
  }

  @Test(expected = AerospikeException.class)
  public void testEmptyNamespace() throws Exception {
    AerospikeDTarget config = getDefaultConfig();
    Target target = config.createTarget();
    TargetRunner runner = new TargetRunner.Builder(AerospikeDTarget.class, target).setOnRecordError(OnRecordError.TO_ERROR).build();
    List<Record> records = new LinkedList<Record>();
    records.add(getTestRecord("", "", "emptyNamespace", ""));
    runner.runInit();
    runner.runWrite(records);
    assertTrue("Record contains empty namespace", runner.getErrorRecords().stream().anyMatch(record -> record.getHeader().getErrorMessage().contains("Invalid namespace")));
    getRecord("", "", "emptyNamespace");
  }

  @Test(expected = AerospikeException.class)
  public void testNonExistentNamespace() throws Exception {
    AerospikeDTarget config = getDefaultConfig();
    Target target = config.createTarget();
    TargetRunner runner = new TargetRunner.Builder(AerospikeDTarget.class, target).setOnRecordError(OnRecordError.TO_ERROR).build();
    List<Record> records = new LinkedList<Record>();
    records.add(getTestRecord("notExists", "", "notExists", ""));
    runner.runInit();
    runner.runWrite(records);
    assertTrue("Record contains non-valid namespace", runner.getErrorRecords().stream().anyMatch(record -> record.getHeader().getErrorMessage().contains("Invalid namespace")));
    getRecord("notExists", "", "notExists");
  }

  @Test
  public void testEmptyKey() throws Exception {
    AerospikeDTarget config = getDefaultConfig();
    Target target = config.createTarget();
    TargetRunner runner = new TargetRunner.Builder(AerospikeDTarget.class, target).setOnRecordError(OnRecordError.TO_ERROR).build();
    List<Record> records = new LinkedList<Record>();
    records.add(getTestRecord("test", "", "", ""));
    runner.runInit();
    runner.runWrite(records);
    assertTrue("Target runs without errors", runner.getErrorRecords().isEmpty());
    com.aerospike.client.Record asRecord = getRecord("test", "", "");
    assertFalse("Aerospike server contains data for empty key.", asRecord.bins.isEmpty());
    assertEquals("Aerospike server contains data for empty key.", 1, asRecord.bins.size());
    assertTrue("Aerospike server contains data for empty key.", asRecord.bins.containsKey("1"));
    assertEquals("Aerospike server contains data for empty key", "1", asRecord.bins.get("1"));

  }

  @Test
  public void testNonEmptyKey() throws Exception {
    AerospikeDTarget config = getDefaultConfig();
    Target target = config.createTarget();
    TargetRunner runner = new TargetRunner.Builder(AerospikeDTarget.class, target).setOnRecordError(OnRecordError.TO_ERROR).build();
    List<Record> records = new LinkedList<Record>();
    records.add(getTestRecord("test", "", "nonEmpty", ""));
    runner.runInit();
    runner.runWrite(records);
    assertTrue("Target runs without errors", runner.getErrorRecords().isEmpty());
    com.aerospike.client.Record asRecord = getRecord("test", "", "nonEmpty");
    assertFalse("Aerospike server contains data for non-empty key.", asRecord.bins.isEmpty());
    assertEquals("Aerospike server contains data for non-empty key", 1, asRecord.bins.size());
    assertTrue("Aerospike server contains data for non-empty key", asRecord.bins.containsKey("1"));
    assertEquals("Aerospike server contains data for non-empty key", "1", asRecord.bins.get("1"));
  }

  @Test
  public void testSet() throws Exception {
    AerospikeDTarget config = getDefaultConfig();
    Target target = config.createTarget();
    TargetRunner runner = new TargetRunner.Builder(AerospikeDTarget.class, target).setOnRecordError(OnRecordError.TO_ERROR).build();
    List<Record> records = new LinkedList<Record>();
    records.add(getTestRecord("test", "", "emptySet", ""));
    records.add(getTestRecord("test", "nonEmpty", "nonEmptySet", ""));
    runner.runInit();
    runner.runWrite(records);
    assertTrue("Target runs without errors", runner.getErrorRecords().isEmpty());
    com.aerospike.client.Record asRecord1 = getRecord("test", "", "emptySet");
    com.aerospike.client.Record asRecord2 = getRecord("test", "nonEmpty", "nonEmptySet");
    assertNotNull(asRecord1.bins);
    assertNotNull(asRecord2.bins);
    assertFalse("Aerospike server contains data for empty set.", asRecord1.bins.isEmpty());
    assertFalse("Aerospike server contains data for non-empty set.", asRecord2.bins.isEmpty());

    assertEquals("Aerospike server contains data for empty set", 1, asRecord1.bins.size());
    assertEquals("Aerospike server contains data for non-empty set", 1, asRecord2.bins.size());

    assertTrue("Aerospike server contains data for empty set", asRecord1.bins.containsKey("1"));
    assertTrue("Aerospike server contains data for non-empty set", asRecord2.bins.containsKey("1"));

    assertEquals("Aerospike server contains data for empty set", "1", asRecord1.bins.get("1"));
    assertEquals("Aerospike server contains data for non-empty set", "1", asRecord1.bins.get("1"));

  }
  @Test
  public void testBins() throws Exception {
    AerospikeDTarget config = getDefaultConfig();
    Target target = config.createTarget();
    config.binConfigsEL.add(new BinConfig("${record:value('/bName2')}", "${record:value('/bValue2')}", DataType.LONG));
    config.binConfigsEL.add(new BinConfig("${record:value('/bName3')}", "${record:value('/bValue3')}", DataType.DOUBLE));
    config.binConfigsEL.add(new BinConfig("${record:value('/bName6')}", "${record:value('/bValue4')}", DataType.STRING));
    TargetRunner runner = new TargetRunner.Builder(AerospikeDTarget.class, target).setOnRecordError(OnRecordError.TO_ERROR).build();
    List<Record> records = new LinkedList<Record>();
    records.add(getTestRecord("test", "binTest", "binTest", "bin"));
    runner.runInit();
    runner.runWrite(records);
    assertTrue("Target runs without errors", runner.getErrorRecords().isEmpty());
    com.aerospike.client.Record asRecord = getRecord("test", "binTest", "binTest");
    assertNotNull(asRecord.bins);
    assertFalse("Aerospike server contains data for empty set.", asRecord.bins.isEmpty());

    assertEquals("Aerospike server contains data for empty set", 4, asRecord.bins.size());

    assertTrue("Aerospike server contains data for bin1", asRecord.bins.containsKey("bin1"));
    assertTrue("Aerospike server contains data for bin2", asRecord.bins.containsKey("bin2"));
    assertTrue("Aerospike server contains data for bin3", asRecord.bins.containsKey("bin3"));
    assertTrue("Aerospike server contains data for emptyBin", asRecord.bins.containsKey(""));

    assertEquals("Aerospike server compare data for bin1", "1", asRecord.bins.get("bin1"));
    assertEquals("Aerospike server compare data for bin2", 2L, asRecord.bins.get("bin2"));
    assertEquals("Aerospike server compare data for bin3", 3.0, asRecord.bins.get("bin3"));
    assertEquals("Aerospike server compare data for bin4", "4", asRecord.bins.get(""));
  }
}
