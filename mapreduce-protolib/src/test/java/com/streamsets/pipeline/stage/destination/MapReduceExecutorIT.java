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
package com.streamsets.pipeline.stage.destination;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.ExecutorRunner;
import com.streamsets.pipeline.stage.destination.mapreduce.MapReduceDExecutor;
import com.streamsets.pipeline.stage.destination.mapreduce.MapReduceExecutor;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class MapReduceExecutorIT extends BaseMapReduceIT {

  @Test
  public void testSimpleJobExecutionThatFails() throws Exception{
    MapReduceExecutor executor = generateExecutor(ImmutableMap.<String, String>builder()
      .put("mapreduce.job.inputformat.class", SimpleTestInputFormat.class.getCanonicalName())
      .put("mapreduce.output.fileoutputformat.outputdir", getOutputDir())
      .put(SimpleTestInputFormat.THROW_EXCEPTION, "true")
    .build());

    ExecutorRunner runner = new ExecutorRunner.Builder(MapReduceDExecutor.class, executor)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    Record record = RecordCreator.create();
    record.set(Field.create(ImmutableMap.of(
      "key", Field.create("value")
    )));

    runner.runWrite(ImmutableList.of(record));

    Assert.assertEquals(1, runner.getErrorRecords().size());

    runner.runDestroy();
  }

  @Test
  public void testSimpleJobExecution() throws Exception{
    File validationFile = new File(getInputDir(), "created");
    MapReduceExecutor executor = generateExecutor(ImmutableMap.<String, String>builder()
      .put("mapreduce.job.inputformat.class", SimpleTestInputFormat.class.getCanonicalName())
      .put("mapreduce.output.fileoutputformat.outputdir", getOutputDir())
      .put(SimpleTestInputFormat.FILE_LOCATION, validationFile.getAbsolutePath())
      .put(SimpleTestInputFormat.FILE_VALUE, "secret")
    .build());

    ExecutorRunner runner = new ExecutorRunner.Builder(MapReduceDExecutor.class, executor)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    Record record = RecordCreator.create();
    record.set(Field.create(ImmutableMap.of(
      "key", Field.create("value")
    )));

    runner.runWrite(ImmutableList.of(record));
    Assert.assertEquals(0, runner.getErrorRecords().size());

    // Event should be properly generated
    Assert.assertEquals(1, runner.getEventRecords().size());
    Record event = runner.getEventRecords().get(0);
    Assert.assertNotNull(event.get("/job-id").getValueAsString());

    // And we should create a special mark file while starting the mr job
    Assert.assertTrue(validationFile.exists());
    Assert.assertEquals("secret", FileUtils.readFileToString(validationFile));

    runner.runDestroy();
  }

  @Test
  public void testSimpleJobExecutionEvaluateExpression() throws Exception{
    File validationFile = new File(getInputDir(), "created");
    MapReduceExecutor executor = generateExecutor(ImmutableMap.<String, String>builder()
      .put("mapreduce.job.inputformat.class", SimpleTestInputFormat.class.getCanonicalName())
      .put("mapreduce.output.fileoutputformat.outputdir", getOutputDir())
      .put(SimpleTestInputFormat.FILE_LOCATION, validationFile.getAbsolutePath())
      .put(SimpleTestInputFormat.FILE_VALUE, "${record:value('/key')}")
    .build());

    ExecutorRunner runner = new ExecutorRunner.Builder(MapReduceDExecutor.class, executor)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    Record record = RecordCreator.create();
    record.set(Field.create(ImmutableMap.of(
      "key", Field.create("value")
    )));

    runner.runWrite(ImmutableList.of(record));
    Assert.assertEquals(0, runner.getErrorRecords().size());

    // Event should be properly generated
    Assert.assertEquals(1, runner.getEventRecords().size());
    Record event = runner.getEventRecords().get(0);
    Assert.assertNotNull(event.get("/job-id").getValueAsString());

    // And we should create a special mark file while starting the mr job
    Assert.assertTrue(validationFile.exists());
    Assert.assertEquals("value", FileUtils.readFileToString(validationFile));

    runner.runDestroy();
  }
}
