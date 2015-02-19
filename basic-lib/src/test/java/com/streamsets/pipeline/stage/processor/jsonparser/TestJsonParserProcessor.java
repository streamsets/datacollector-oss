/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.jsonparser;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
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
    ProcessorRunner runner = new ProcessorRunner.Builder(JsonParserProcessor.class)
        .addConfiguration("fieldPathToParse", "/json")
        .addConfiguration("parsedFieldPath", "/parsed")
        .addConfiguration("onRecordProcessingError", OnRecordProcessingError.DISCARD)
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
      Assert.assertEquals(1, output.getRecords().get("out").size());
      Assert.assertEquals(r0.get("/json").getValueAsString(),
                          output.getRecords().get("out").get(0).get("/json").getValue());
      Assert.assertEquals("A", output.getRecords().get("out").get(0).get("/parsed/a").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testParsingToError() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(JsonParserProcessor.class)
        .addConfiguration("fieldPathToParse", "/json")
        .addConfiguration("parsedFieldPath", "/parsed")
        .addConfiguration("onRecordProcessingError", OnRecordProcessingError.TO_ERROR)
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
      Assert.assertEquals(1, output.getRecords().get("out").size());
      Assert.assertEquals(r0.get("/json").getValueAsString(),
                          output.getRecords().get("out").get(0).get("/json").getValue());
      Assert.assertEquals("A", output.getRecords().get("out").get(0).get("/parsed/a").getValue());
      Assert.assertEquals(4, runner.getErrorRecords().size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test(expected = StageException.class)
  public void testParsingStopPipeline() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(JsonParserProcessor.class)
        .addConfiguration("fieldPathToParse", "/json")
        .addConfiguration("parsedFieldPath", "/parsed")
        .addConfiguration("onRecordProcessingError", OnRecordProcessingError.STOP_PIPELINE)
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
