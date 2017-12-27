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
package com.streamsets.pipeline.stage.processor.generator;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.sdk.service.SdkJsonDataFormatGeneratorService;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestDataGeneratorProcessor {

  @Test
  public void generateString() throws Exception {
    DataGeneratorConfig config = new DataGeneratorConfig();
    config.outputType = OutputType.STRING;
    config.targetField = "/";

    ProcessorRunner runner = new ProcessorRunner.Builder(DataGeneratorDProcessor.class, new DataGeneratorProcessor(config))
      .addOutputLane("out")
      .addService(DataFormatGeneratorService.class, new SdkJsonDataFormatGeneratorService())
      .build();

    runner.runInit();
    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("StreamSets Inc."));
      map.put("age", Field.create(Field.Type.LONG, 4));

      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      List<Record> outputRecords = output.getRecords().get("out");
      assertEquals(1, outputRecords.size());

      Record outputRecord = outputRecords.get(0);
      assertTrue(outputRecord.has("/"));
      assertFalse(outputRecord.has("/name"));
      assertFalse(outputRecord.has("/age"));

      Field rootField = outputRecord.get();
      assertEquals(Field.Type.STRING, rootField.getType());
      assertEquals("{\"name\":\"StreamSets Inc.\",\"age\":4}", rootField.getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }
}
