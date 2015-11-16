/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.lib.influxdb;
//
//import com.streamsets.datacollector.runner.BatchImpl;
//import com.streamsets.pipeline.api.Batch;
//import com.streamsets.pipeline.api.Field;
//import com.streamsets.pipeline.api.Record;
//import com.streamsets.pipeline.sdk.RecordCreator;
import org.influxdb.InfluxDB;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestInfluxBatchWriter {
  @Test
  public void testWriteBatch() throws Exception {
//    InfluxBatchWriter writer = new InfluxBatchWriter("http://boot2docker:8086", "admin", "admin", "collectd", InfluxDB.ConsistencyLevel.ALL, "default");
//
//    List<Record> records = new ArrayList<>();
//
//    Record record = RecordCreator.create();
//    Map<String, Field> rootMap = new HashMap<>();
//    rootMap.put("time_hires", Field.create(1554363717440187943L));
//    rootMap.put("host", Field.create("ip-192-168-42-238.us-west-2.compute.internal"));
//    rootMap.put("plugin", Field.create("interface"));
//    rootMap.put("plugin_instance", Field.create("utun0"));
//    rootMap.put("rx", Field.create(82));
//    rootMap.put("tx", Field.create(107));
//    rootMap.put("type", Field.create("if_packets"));
//    Field root = Field.create(rootMap);
//    record.set(root);
//    records.add(record);
//
//    Batch batch = new BatchImpl("test", "0", records);
//
//    long count = writer.writeBatch(batch);
//
//    Assert.assertEquals(1, count);
  }
}
