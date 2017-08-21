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
package com.streamsets.pipeline.lib;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.destination.flume.ClientType;
import com.streamsets.pipeline.stage.destination.flume.FlumeConfig;
import com.streamsets.pipeline.stage.destination.flume.FlumeConfigBean;
import com.streamsets.pipeline.stage.destination.flume.FlumeTarget;
import com.streamsets.pipeline.stage.destination.flume.HostSelectionStrategy;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FlumeTestUtil {

  private FlumeTestUtil() {}

  private static final String MIME = "text/plain";
  private static final String TEST_STRING = "Hello World";
  public static final String TEST_STRING_255 = "StreamSets was founded in June 2014 by business and engineering " +
    "leaders in the data integration space with a history of bringing successful products to market. We’re a " +
    "team that is laser-focused on solving hard problems so our customers don’t have to.";


  public static List<Record> produce20Records() throws IOException {
    List<Record> list = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      Record record = RecordCreator.create();
      Map<String, Field> map = new HashMap<>();
      map.put("name", Field.create("NAME" + i));
      map.put("lastStatusChange", Field.create(i));
      record.set(Field.create(map));
      list.add(record);
    }
    return list;
  }

  public static List<Record> createJsonRecords() throws IOException {
    return produce20Records();
  }

  public static List<Record> createStringRecords() {
    List<Record> records = new ArrayList<>(9);
    for (int i = 0; i < 9; i++) {
      Record r = RecordCreator.create("s", "s:1", (TEST_STRING + i).getBytes(), MIME);
      r.set(Field.create((TEST_STRING + i)));
      records.add(r);
    }
    return records;
  }

  public static List<Record> createCsvRecords() throws IOException {
    List<Record> records = new ArrayList<>();
    String line;
    BufferedReader bufferedReader = new BufferedReader(new FileReader(FlumeTestUtil.class.getClassLoader()
      .getResource("testFlumeTarget.csv").getFile()));
    while ((line = bufferedReader.readLine()) != null) {
      String columns[] = line.split(",");
      List<Field> list = new ArrayList<>();
      for (String column : columns) {
        Map<String, Field> map = new LinkedHashMap<>();
        map.put("value", Field.create(column));
        list.add(Field.create(map));
      }
      Record record = RecordCreator.create("s", "s:1", null, null);
      record.set(Field.create(list));
      records.add(record);
    }
    return records;
  }

  public static List<Record> createBinaryRecords() {
    List<Record> records = new ArrayList<>(9);
    for (int i = 0; i < 9; i++) {
      Record record = RecordCreator.create();
      Map<String, Field> map = new HashMap<>();
      map.put("data", Field.create((TEST_STRING_255 + i).getBytes()));
      record.set(Field.create(map));
      records.add(record);
    }
    return records;
  }

  public static FlumeTarget createFlumeTarget(
    FlumeConfig flumeConfig,
    DataFormat dataFormat,
    DataGeneratorFormatConfig dataGeneratorFormatConfig
  ) {
    FlumeConfigBean flumeConfigBean = new FlumeConfigBean();
    flumeConfigBean.flumeConfig = flumeConfig;
    flumeConfigBean.dataFormat = dataFormat;
    flumeConfigBean.dataGeneratorFormatConfig = dataGeneratorFormatConfig;
    return new FlumeTarget(flumeConfigBean);
  }

  public static FlumeConfig createFlumeConfig(
    boolean backoff,
    int batchSize,
    ClientType clientType,
    int connectionTimeout,
    Map<String, String> flumeHostsConfig,
    HostSelectionStrategy hostSelectionStrategy,
    int maxBackOff,
    int maxRetryAttempts,
    int requestTimeout,
    boolean singleEventPerBatch,
    int waitBetweenRetries
  ) {
    FlumeConfig flumeConfig = new FlumeConfig();
    flumeConfig.backOff = backoff;
    flumeConfig.batchSize = batchSize;
    flumeConfig.clientType = clientType;
    flumeConfig.connectionTimeout = connectionTimeout;
    flumeConfig.flumeHostsConfig = flumeHostsConfig;
    flumeConfig.hostSelectionStrategy = hostSelectionStrategy;
    flumeConfig.maxBackOff = maxBackOff;
    flumeConfig.maxRetryAttempts = maxRetryAttempts;
    flumeConfig.requestTimeout = requestTimeout;
    flumeConfig.singleEventPerBatch = singleEventPerBatch;
    flumeConfig.waitBetweenRetries = waitBetweenRetries;
    return flumeConfig;
  }

  public static FlumeConfig createDefaultFlumeConfig(int port, boolean singleEvent) {
    return createFlumeConfig(
      false,                      // backOff
      1,                          // batchSize
      ClientType.AVRO_FAILOVER,
      1000,                       // connection timeout
      ImmutableMap.of("h1", "localhost:" + port),
      HostSelectionStrategy.RANDOM,
      0,                          // maxBackOff
      3,                          // maxRetryAttempts
      1000,                       // requestTimeout
      singleEvent,                // singleEventPerBatch
      0
    );
  }
}
