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
package com.streamsets.pipeline.stage.processor.base64;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import static com.streamsets.pipeline.stage.processor.base64.Utils.ORIGINAL_PATH;
import static com.streamsets.pipeline.stage.processor.base64.Utils.createRecord;

abstract class Base64TestBase {

  private Processor processor;
  private Class dProcessorClass;
  private byte[] originalData;
  private Field originalField;

  protected Base64TestBase(Processor processor, Class dProcessorClass, byte[] originalData) {
    this.processor = processor;
    this.dProcessorClass = dProcessorClass;
    this.originalData = originalData;
  }

  // The following four tests are common for both the processors.
  @Test
  public void testSuccess() throws Exception {
    RunnerAndOutput ro = doTest(OnRecordError.STOP_PIPELINE, false, false);
    verify(ro.output.getRecords().get("base64").get(0));
  }

  @Test
  public void testInvalidFieldTypeDiscard() throws Exception {
    doTestInvalidFieldDiscard(true, false);
  }

  @Test(expected = OnRecordErrorException.class)
  public void testInvalidFieldTypeStopPipeLine() throws Exception {
    doTestInvalidFieldStopPipeLine(true, false);
  }

  @Test
  public void testInvalidFieldTypeToError() throws Exception {
    doTestInvalidFieldToError(true, false);
  }

  protected void doTestInvalidFieldDiscard(boolean invalidFormat, boolean invalidData) throws Exception {
    RunnerAndOutput ro = doTest(OnRecordError.DISCARD, invalidFormat, invalidData);
    Assert.assertEquals(ro.output.getRecords().get("base64").size(), 0);
  }

  protected void doTestInvalidFieldStopPipeLine(boolean invalidFormat, boolean invalidData) throws Exception {
    doTest(OnRecordError.STOP_PIPELINE, invalidFormat, invalidData);
  }

  protected void doTestInvalidFieldToError(boolean invalidFormat, boolean invalidData) throws Exception {
    RunnerAndOutput ro = doTest(OnRecordError.TO_ERROR, invalidFormat, invalidData);
    Assert.assertTrue(ro.runner.getErrorRecords().get(0).get(ORIGINAL_PATH).equals(originalField));
  }

  private RunnerAndOutput doTest(
      OnRecordError onRecordError,
      boolean invalidFormat,
      boolean invalidData
  ) throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(dProcessorClass, processor)
        .addOutputLane("base64").setOnRecordError(onRecordError).build();
    runner.runInit();
    Record record = createRecord();
    if (invalidFormat) {
      originalField = Field.create("testString");
    } else if (invalidData) {
      originalField = Field.create(getInvalidData());
    } else {
      originalField = Field.create(originalData);
    }
    record.set(ORIGINAL_PATH, originalField);
    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    return new RunnerAndOutput(runner, output);
  }

  /**
   * Given the processed record, this method verifies if it has all the fields as expected.
   */
  protected abstract void verify(Record record);

  /**
   * Generate some data that is invalid as the original value to be decoded
   */
  protected abstract byte[] getInvalidData();

  private class RunnerAndOutput {
    ProcessorRunner runner;
    StageRunner.Output output;

    public RunnerAndOutput(ProcessorRunner runner, StageRunner.Output output) {
      this.runner = runner;
      this.output = output;
    }
  }

}
