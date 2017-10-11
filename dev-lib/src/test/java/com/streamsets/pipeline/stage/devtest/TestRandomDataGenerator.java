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
package com.streamsets.pipeline.stage.devtest;

import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.junit.Assert;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestRandomDataGenerator {

  @Test
  public void testRandomDataGenerator() throws Exception {

    RandomDataGeneratorSource.DataGeneratorConfig stringData = new RandomDataGeneratorSource.DataGeneratorConfig();
    stringData.field = "name";
    stringData.type = RandomDataGeneratorSource.Type.STRING;

    RandomDataGeneratorSource.DataGeneratorConfig intData = new RandomDataGeneratorSource.DataGeneratorConfig();
    intData.field = "age";
    intData.type = RandomDataGeneratorSource.Type.INTEGER;

    RandomDataGeneratorSource.DataGeneratorConfig longData = new RandomDataGeneratorSource.DataGeneratorConfig();
    longData.field = "milliSecondsSinceBirth";
    longData.type = RandomDataGeneratorSource.Type.LONG;

    RandomDataGeneratorSource.DataGeneratorConfig dateData = new RandomDataGeneratorSource.DataGeneratorConfig();
    dateData.field = "dob";
    dateData.type = RandomDataGeneratorSource.Type.DATE;

    RandomDataGeneratorSource.DataGeneratorConfig doubleData = new RandomDataGeneratorSource.DataGeneratorConfig();
    doubleData.field = "salary";
    doubleData.type = RandomDataGeneratorSource.Type.DOUBLE;

    RandomDataGeneratorSource.DataGeneratorConfig decimalData = new RandomDataGeneratorSource.DataGeneratorConfig();
    decimalData.field = "decimal";
    decimalData.type = RandomDataGeneratorSource.Type.DECIMAL;
    decimalData.scale = 2;
    decimalData.precision = 5;

    RandomDataGeneratorSource.DataGeneratorConfig zonedDateTimeData =
        new RandomDataGeneratorSource.DataGeneratorConfig();
    zonedDateTimeData.field = "zonedDateTime";
    zonedDateTimeData.type = RandomDataGeneratorSource.Type.ZONED_DATETIME;

    final PushSourceRunner runner = new PushSourceRunner.Builder(RandomDataGeneratorSource.class)
      .addConfiguration("dataGenConfigs",
          Arrays.asList(stringData, dateData, doubleData, longData, intData, decimalData, zonedDateTimeData))
      .addConfiguration("rootFieldType", RandomDataGeneratorSource.RootType.MAP)
      .addConfiguration("delay", 0)
      .addConfiguration("batchSize", 1000)
      .addConfiguration("numThreads", 1)
      .addConfiguration("eventName", "secret-name")
      .addOutputLane("a")
      .build();
    runner.runInit();
    try {
      final List<Record> records = new ArrayList<>();
      runner.runProduce(Collections.<String, String>emptyMap(), 1, new PushSourceRunner.Callback() {
        @Override
        public void processBatch(StageRunner.Output output) {
          records.clear();
          records.addAll(output.getRecords().get("a"));
          runner.setStop();
        }
      });
      runner.waitOnProduce();

      Assert.assertEquals(1, records.size());
      Record record =  records.get(0);
      Assert.assertEquals(Field.Type.STRING, record.get("/name").getType());
      Assert.assertEquals(Field.Type.INTEGER, record.get("/age").getType());
      Assert.assertEquals(Field.Type.LONG, record.get("/milliSecondsSinceBirth").getType());
      Assert.assertEquals(Field.Type.DATE, record.get("/dob").getType());
      Assert.assertEquals(Field.Type.DOUBLE, record.get("/salary").getType());

      Field decimalField = record.get("/decimal");
      Assert.assertNotNull(decimalData);
      Assert.assertEquals(Field.Type.DECIMAL, decimalField.getType());
      Assert.assertEquals("5", decimalField.getAttribute(HeaderAttributeConstants.ATTR_PRECISION));
      Assert.assertEquals("2", decimalField.getAttribute(HeaderAttributeConstants.ATTR_SCALE));

      Assert.assertEquals(Field.Type.ZONED_DATETIME, record.get("/zonedDateTime").getType());
      Assert.assertTrue(record.get("/zonedDateTime").getValue() instanceof ZonedDateTime);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testLongSequence() throws Exception {
    RandomDataGeneratorSource.DataGeneratorConfig seq = new RandomDataGeneratorSource.DataGeneratorConfig();
    seq.field = "id";
    seq.type = RandomDataGeneratorSource.Type.LONG_SEQUENCE;

    final PushSourceRunner runner = new PushSourceRunner.Builder(RandomDataGeneratorSource.class)
      .addConfiguration("dataGenConfigs", Arrays.asList(seq))
      .addConfiguration("rootFieldType", RandomDataGeneratorSource.RootType.MAP)
      .addConfiguration("delay", 0)
      .addConfiguration("batchSize", 1000)
      .addConfiguration("numThreads", 1)
      .addConfiguration("eventName", "secret-name")
      .addOutputLane("a")
      .build();
    runner.runInit();
    try {
      final List<Record> records = new ArrayList<>();
      runner.runProduce(Collections.<String, String>emptyMap(), 1000, new PushSourceRunner.Callback() {
        @Override
        public void processBatch(StageRunner.Output output) {
          records.clear();
          records.addAll(output.getRecords().get("a"));
          runner.setStop();
        }
      });
      runner.waitOnProduce();

      Assert.assertTrue(records.size() > 1);
      for(long i = 0; i < records.size(); i++) {
        Field field = records.get((int)i).get().getValueAsMap().get("id");
        Assert.assertNotNull(field);
        Assert.assertEquals(Field.Type.LONG, field.getType());
        Assert.assertEquals(i, field.getValueAsLong());
      }
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testEventGeneration() throws Exception {
    RandomDataGeneratorSource.DataGeneratorConfig seq = new RandomDataGeneratorSource.DataGeneratorConfig();
    seq.field = "event";
    seq.type = RandomDataGeneratorSource.Type.LONG_SEQUENCE;

    final PushSourceRunner runner = new PushSourceRunner.Builder(RandomDataGeneratorSource.class)
      .addConfiguration("dataGenConfigs", Arrays.asList(seq))
      .addConfiguration("rootFieldType", RandomDataGeneratorSource.RootType.MAP)
      .addConfiguration("delay", 0)
      .addConfiguration("batchSize", 1000)
      .addConfiguration("numThreads", 1)
      .addConfiguration("eventName", "secret-name")
      .addOutputLane("a")
      .build();
    runner.runInit();
    try {
      runner.runProduce(Collections.<String, String>emptyMap(), 1000, output -> runner.setStop());
      runner.waitOnProduce();

      List<Record> records = runner.getEventRecords();
      Assert.assertTrue(records.size() > 1);
      for(long i = 0; i < records.size(); i++) {
        Record r = records.get((int)i);

        // Validate header
        Assert.assertEquals("secret-name", r.getHeader().getAttribute(EventRecord.TYPE));

        // Validate field
        Field field = r.get().getValueAsMap().get("event");
        Assert.assertNotNull(field);
        Assert.assertEquals(Field.Type.LONG, field.getType());
        Assert.assertEquals(i, field.getValueAsLong());
      }
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testBatchSize() throws Exception {
    RandomDataGeneratorSource.DataGeneratorConfig seq = new RandomDataGeneratorSource.DataGeneratorConfig();
    seq.field = "id";
    seq.type = RandomDataGeneratorSource.Type.LONG_SEQUENCE;

    final PushSourceRunner runner = new PushSourceRunner.Builder(RandomDataGeneratorSource.class)
      .addConfiguration("dataGenConfigs", Arrays.asList(seq))
      .addConfiguration("rootFieldType", RandomDataGeneratorSource.RootType.MAP)
      .addConfiguration("delay", 0)
      .addConfiguration("batchSize", 1)
      .addConfiguration("numThreads", 1)
      .addConfiguration("eventName", "secret-name")
      .addOutputLane("a")
      .build();
    runner.runInit();
    try {
      final List<Record> records = new ArrayList<>();
      runner.runProduce(Collections.<String, String>emptyMap(), 1000, new PushSourceRunner.Callback() {
        @Override
        public void processBatch(StageRunner.Output output) {
          records.clear();
          records.addAll(output.getRecords().get("a"));
          runner.setStop();
        }
      });
      runner.waitOnProduce();

      Assert.assertEquals(1, records.size());
    } finally {
      runner.runDestroy();
    }
  }
}
