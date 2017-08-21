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
package com.streamsets.pipeline.stage.processor.jsonparser;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestJsonParserProcessor {

  private Record createRecordWithLine(String jsonString) {
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("json", Field.create(jsonString));
    record.set(Field.create(map));
    return record;
  }

  @Test
  public void testParsingDiscarding() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(JsonParserDProcessor.class)
        .setOnRecordError(OnRecordError.DISCARD)
        .addConfiguration("removeCtrlChars", false)
        .addConfiguration("fieldPathToParse", "/json")
        .addConfiguration("parsedFieldPath", "/parsed")
        .addOutputLane("out")
        .build();
    runner.runInit();
    try {

      Record r0 = createRecordWithLine("{\"a\":\"A\"}");
      Record r1 = createRecordWithLine(null);
      Record r2 = createRecordWithLine("{");
      Record r3 = createRecordWithLine("");
      Record r4 = RecordCreator.create();
      List<Record> input = ImmutableList.of(r0, r1, r2, r3, r4);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(2, output.getRecords().get("out").size());
      Assert.assertEquals(r0.get("/json").getValueAsString(),
                          output.getRecords().get("out").get(0).get("/json").getValue());
      Assert.assertEquals("A", output.getRecords().get("out").get(0).get("/parsed/a").getValue());
      Assert.assertEquals(r3.get("/json").getValueAsString(),
                          output.getRecords().get("out").get(1).get("/json").getValue());
      Assert.assertFalse(output.getRecords().get("out").get(1).has("/parsed/a"));
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testParsingToError() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(JsonParserDProcessor.class)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addConfiguration("removeCtrlChars", false)
        .addConfiguration("fieldPathToParse", "/json")
        .addConfiguration("parsedFieldPath", "/parsed")
        .addOutputLane("out")
        .build();
    runner.runInit();
    try {
      Record r0 = createRecordWithLine("{\"a\":\"A\"}");
      Record r1 = createRecordWithLine(null);
      Record r2 = createRecordWithLine("{");
      Record r3 = createRecordWithLine("");
      Record r4 = RecordCreator.create();
      List<Record> input = ImmutableList.of(r0, r1, r2, r3, r4);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(2, output.getRecords().get("out").size());
      Assert.assertEquals(r0.get("/json").getValueAsString(),
                          output.getRecords().get("out").get(0).get("/json").getValue());
      Assert.assertEquals("A", output.getRecords().get("out").get(0).get("/parsed/a").getValue());
      Assert.assertEquals(r3.get("/json").getValueAsString(),
                          output.getRecords().get("out").get(1).get("/json").getValue());
      Assert.assertFalse(output.getRecords().get("out").get(1).has("/parsed/a"));
      Assert.assertEquals(3, runner.getErrorRecords().size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test(expected = OnRecordErrorException.class)
  public void testParsingStopPipeline() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(JsonParserDProcessor.class)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .addConfiguration("removeCtrlChars", false)
        .addConfiguration("fieldPathToParse", "/json")
        .addConfiguration("parsedFieldPath", "/parsed")
        .addOutputLane("out")
        .build();
    runner.runInit();
    try {
      Record r0 = createRecordWithLine("{\"a\":\"A\"}");
      Record r1 = createRecordWithLine(null);
      List<Record> input = ImmutableList.of(r0, r1);
      runner.runProcess(input);
    } finally {
      runner.runDestroy();
    }
  }

}
