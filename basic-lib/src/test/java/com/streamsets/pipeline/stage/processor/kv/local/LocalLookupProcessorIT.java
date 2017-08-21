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
package com.streamsets.pipeline.stage.processor.kv.local;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.processor.kv.LookupMode;
import com.streamsets.pipeline.stage.processor.kv.LookupParameterConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.streamsets.pipeline.stage.processor.kv.LookupMode.BATCH;
import static com.streamsets.pipeline.stage.processor.kv.LookupMode.RECORD;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class LocalLookupProcessorIT {
  @Parameterized.Parameters
  public static Collection<LookupMode> modes() {
    return ImmutableList.of(RECORD, BATCH);
  }

  @Parameterized.Parameter
  public LookupMode mode;

  @Test
  public void testEmptyBatch() throws Exception {
    Processor processor = getDefaultProcessor();

    List<Record> records = new ArrayList<>();

    ProcessorRunner runner = new ProcessorRunner.Builder(LocalLookupDProcessor.class, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    runner.runProcess(records);

    assertTrue(runner.getErrorRecords().isEmpty());
  }

  @Test
  public void testSingleExpressionBatch() throws Exception {
    Processor processor = getDefaultProcessor();

    List<Record> records = getRecords(4);

    ProcessorRunner runner = new ProcessorRunner.Builder(LocalLookupDProcessor.class, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    StageRunner.Output output = runner.runProcess(records);

    assertTrue(runner.getErrorRecords().isEmpty());
    assertArrayEquals(getExpectedRecords(4).toArray(), output.getRecords().get("lane").toArray());
  }

  private List<Record> getRecords(int i) {
    List<Record> records = new ArrayList<>();
    for (int j = 1; j <= i; j++) {
      Record record = RecordCreator.create();
      Map<String, Field> fields = new HashMap<>();
      fields.put("keyField", Field.create("key" + j));
      record.set(Field.create(fields));
      records.add(record);
    }
    return records;
  }

  private List<Record> getExpectedRecords(int i) {
    List<Record> records = new ArrayList<>();
    for (int j = 1; j <= i; j++) {
      Record record = RecordCreator.create();
      Map<String, Field> fields = new HashMap<>();
      fields.put("keyField", Field.create("key" + j));
      // Don't set a value for the last one as it's absent.
      if (j != i) {
        fields.put("output", Field.create("value" + j));
      }
      record.set(Field.create(fields));
      records.add(record);
    }
    return records;
  }

  private Processor getDefaultProcessor() {
    LocalLookupConfig conf = new LocalLookupConfig();
    conf.values = ImmutableMap.of("key1", "value1", "key2", "value2", "key3", "value3");
    LookupParameterConfig parameters = new LookupParameterConfig();
    parameters.keyExpr = "${record:value('/keyField')}";
    parameters.outputFieldPath = "/output";
    conf.lookups.add(parameters);
    conf.mode = mode;
    return new LocalLookupProcessor(conf);
  }
}
