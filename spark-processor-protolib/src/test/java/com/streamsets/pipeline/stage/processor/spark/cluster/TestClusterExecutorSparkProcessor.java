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
package com.streamsets.pipeline.stage.processor.spark.cluster;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.processor.spark.SparkDProcessor;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class TestClusterExecutorSparkProcessor {
  private static final String LANE = "spark";

  @Test
  @SuppressWarnings("unchecked")
  public void testProcessor() throws Exception {
    final OnRecordError onRecordError = OnRecordError.TO_ERROR;

    ClusterExecutorSparkProcessor processor = new ClusterExecutorSparkProcessor();

    final List<Record> records = new ArrayList<>(100);
    for (int i = 0; i < 100; i++) {
      Record r = RecordCreator.create();
      records.add(r);
      r.set(Field.create(new HashMap<>()));
      r.set("/value", Field.create(i));
    }

    final ProcessorRunner runner = new ProcessorRunner.Builder(SparkDProcessor.class, processor)
        .addOutputLane(LANE).setOnRecordError(onRecordError).build();

    final AtomicReference<StageRunner.Output> output = new AtomicReference<>();
    runner.runInit();
    Executor executor = Executors.newSingleThreadExecutor();
    executor.execute(() -> {
      try {
        output.set(runner.runProcess(records));
      } catch (Exception ex) {
        throw Throwables.propagate(ex);
      }
    });
    Record[] dataFromProcessor = Iterators.toArray(processor.getBatch(), Record.class);
    Iterator<Record> inputBatchReceived = Iterators.forArray(dataFromProcessor);
    for (Record record : records) {
      Assert.assertTrue(inputBatchReceived.hasNext());
      Assert.assertEquals(record.get(), inputBatchReceived.next().get());
    }

    Iterator transformed = Iterators.transform(
        Iterators.forArray(Arrays.copyOfRange(dataFromProcessor, 0, dataFromProcessor.length - 5)),
        record -> {
          Record newR = RecordCreator.create();
          newR.set(Field.create(new HashMap<>()));
          newR.set("/value", Field.create(record.get("/value").getValueAsInteger() + 100));
          return newR;
        });

    Record[] errors = Arrays.copyOfRange(dataFromProcessor, dataFromProcessor.length - 5, dataFromProcessor.length);

    processor.setErrors(Iterators.forArray(errors));
    processor.continueProcessing(transformed);

    Thread.sleep(1000);

    Iterator<Record> finalOutput = output.get().getRecords().get(LANE).iterator();
    for (Record record : records.subList(0, records.size() - 5)) {
      Assert.assertTrue(finalOutput.hasNext());
      Assert.assertEquals(record.get("/value").getValueAsInteger() + 100, finalOutput.next().get("/value").getValueAsInteger());
    }

    Iterator<Record> errorRecords = runner.getErrorRecords().iterator();
    for (Record record : Arrays.asList(dataFromProcessor).subList(dataFromProcessor.length - 5, dataFromProcessor.length)) {
      Assert.assertTrue(errorRecords.hasNext());
      Assert.assertEquals(record.get("/value"), errorRecords.next().get("/value"));
    }
  }

}
