/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.jython;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestJythonProcessor {

  @Test
  public void testOutErr() throws Exception {
    Processor processor = new JythonProcessor(ProcessingMode.RECORD,
                                              "for record in records:\n" +
                                              "  out.write(record)\n" +
                                              "  record.set('/', Field.create('Bye'))\n" +
                                              "  out.write(record)\n" +
                                              "  record.set('/', Field.create('Error'))\n" +
                                              "  err.write(record, 'error')\n");
    ProcessorRunner runner = new ProcessorRunner.Builder(processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {

      Record record = RecordCreator.create();
      record.set(Field.create("Hello"));
      List<Record> input = ImmutableList.of(record);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(2, output.getRecords().get("lane").size());
      Assert.assertEquals("Hello", output.getRecords().get("lane").get(0).get().getValueAsString());
      Assert.assertEquals("Bye", output.getRecords().get("lane").get(1).get().getValueAsString());
      Assert.assertEquals(1, runner.getErrorRecords().size());
      Assert.assertEquals("Error", runner.getErrorRecords().get(0).get().getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  private void testMode(ProcessingMode mode) throws Exception {
    Processor processor = new JythonProcessor(mode,
                                              "for record in records:\n" +
                                              "  out.write(record)");
    ProcessorRunner runner = new ProcessorRunner.Builder(processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {

      Record record1 = RecordCreator.create();
      record1.set(Field.create("Hello"));
      Record record2 = RecordCreator.create();
      record2.set(Field.create("Bye"));
      List<Record> input = ImmutableList.of(record1, record2);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(2, output.getRecords().get("lane").size());
      Assert.assertEquals("Hello", output.getRecords().get("lane").get(0).get().getValueAsString());
      Assert.assertEquals("Bye", output.getRecords().get("lane").get(1).get().getValueAsString());
      Assert.assertEquals(0, runner.getErrorRecords().size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRecordMode() throws Exception {
    testMode(ProcessingMode.RECORD);
  }

  @Test
  public void testBatchMode() throws Exception {
    testMode(ProcessingMode.BATCH);
  }

  private void testRecordModeOnErrorHandling(OnRecordError  onRecordError) throws Exception {
    Processor processor = new JythonProcessor(ProcessingMode.RECORD,
                                              "for record in records:\n" +
                                              "  if record.get().getValueAsString() == 'Hello':\n" +
                                              "    raise Exception()\n" +
                                              "  out.write(record)");
    ProcessorRunner runner = new ProcessorRunner.Builder(processor)
        .setOnRecordError(onRecordError)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {

      Record record1 = RecordCreator.create();
      record1.set(Field.create("Hello"));
      Record record2 = RecordCreator.create();
      record2.set(Field.create("Bye"));
      List<Record> input = ImmutableList.of(record1, record2);
      StageRunner.Output output = runner.runProcess(input);
      switch (onRecordError) {
        case DISCARD:
          Assert.assertEquals(1, output.getRecords().get("lane").size());
          Assert.assertEquals("Bye", output.getRecords().get("lane").get(0).get().getValueAsString());
          Assert.assertEquals(0, runner.getErrorRecords().size());
          break;
        case TO_ERROR:
          Assert.assertEquals(1, output.getRecords().get("lane").size());
          Assert.assertEquals("Bye", output.getRecords().get("lane").get(0).get().getValueAsString());
          Assert.assertEquals(1, runner.getErrorRecords().size());
          Assert.assertEquals("Hello", runner.getErrorRecords().get(0).get().getValueAsString());
          break;
      }
    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testRecordOnErrorDiscard() throws Exception {
    testRecordModeOnErrorHandling(OnRecordError.DISCARD);
  }

  @Test
  public void testRecordOnErrorToError() throws Exception {
    testRecordModeOnErrorHandling(OnRecordError.TO_ERROR);
  }

  @Test(expected = StageException.class)
  public void testRecordOnErrorStopPipeline() throws Exception {
    testRecordModeOnErrorHandling(OnRecordError.STOP_PIPELINE);
  }

  private void testBatchModeOnErrorHandling(OnRecordError  onRecordError) throws Exception {
    Processor processor = new JythonProcessor(ProcessingMode.BATCH,
                                              "for record in records:\n" +
                                              "  if record.get().getValueAsString() == 'Hello':\n" +
                                              "    raise Exception()\n" +
                                              "  out.write(record)");
    ProcessorRunner runner = new ProcessorRunner.Builder(processor)
        .setOnRecordError(onRecordError)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {

      Record record1 = RecordCreator.create();
      record1.set(Field.create("Hello"));
      Record record2 = RecordCreator.create();
      record2.set(Field.create("Bye"));
      List<Record> input = ImmutableList.of(record1, record2);
      StageRunner.Output output = runner.runProcess(input);
    } finally {
      runner.runDestroy();
    }
  }


  @Test(expected = StageException.class)
  public void testBatchOnErrorDiscard() throws Exception {
    testBatchModeOnErrorHandling(OnRecordError.DISCARD);
  }

  @Test(expected = StageException.class)
  public void testBatchOnErrorToError() throws Exception {
    testBatchModeOnErrorHandling(OnRecordError.TO_ERROR);
  }

  @Test(expected = StageException.class)
  public void testBatchOnErrorStopPipeline() throws Exception {
    testBatchModeOnErrorHandling(OnRecordError.STOP_PIPELINE);
  }

}
