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
package com.streamsets.pipeline.stage.origin.udp;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DatagramMode;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TestMultithreadedUDPSource extends BaseUDPSourceTest {
  private PushSourceRunner pushRunner;
  private MultithreadedUDPSource multithreadedSource;

  @Override
  protected void initializeRunner(UDPSourceConfigBean conf, int numThreads) throws StageException {
    multithreadedSource = new MultithreadedUDPSource(
        conf,
        500,
        numThreads
    );

    pushRunner = new PushSourceRunner.Builder(
        MultithreadedUDPDSource.class,
        multithreadedSource
    ).addOutputLane(OUTPUT_LANE).build();

    pushRunner.runInit();
  }

  @Override
  protected void runProduce(
      DatagramMode dataFormat,
      List<String> ports
  ) throws Exception {
    final int batchSize = 10;

    final int totalExpectedRecords = getNumTotalExpectedRecords(dataFormat, ports);
    final AtomicInteger numRemainingRecords = new AtomicInteger(totalExpectedRecords);
    pushRunner.runProduce(new HashMap<>(), batchSize, new PushSourceRunner.Callback() {
      @Override
      public void processBatch(StageRunner.Output output) {
        final int thisBatchStartIndex = totalExpectedRecords - numRemainingRecords.get();
        final List<Record> records = getOutputRecords(output);
        final int numBatchRecords = records.size();
        Assert.assertTrue("Number of records in batch larger than max batch size", numBatchRecords <= batchSize);
        assertRecordsInBatch(dataFormat, thisBatchStartIndex, records);
        final int remaining = numRemainingRecords.addAndGet(-numBatchRecords);
        if (remaining == 0) {
          pushRunner.setStop();
        }
      }
    });
    pushRunner.waitOnProduce();
  }

  @Override
  protected void destroyRunner() throws StageException {
    pushRunner.runDestroy();
  }

}
