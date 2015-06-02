/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline;

import com.streamsets.pipeline.ClusterQueue;
import com.streamsets.pipeline.ClusterQueueConsumer;
import com.streamsets.pipeline.OffsetAndResult;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.ThreadUtil;

import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Random;

public class TestClusterQueue {
  private static final Logger LOG = LoggerFactory.getLogger(TestClusterQueue.class);
  private static final int MAX_WAIT_TIME = 50;

  private ClusterQueue queue;
  private ClusterQueueConsumer source;
  private MockSourceRunner sourceRunner;

  @Before
  public void setup() {
    queue = new ClusterQueue();
    source = new ClusterQueueConsumer(queue);
    sourceRunner = new MockSourceRunner(source);
  }

  @After
  public void teardown() {
    if (sourceRunner != null) {
      sourceRunner.stop = true;
      sourceRunner.interrupt();
    }
  }

  static class SomeException extends RuntimeException {
    SomeException(String msg) {
      super(msg);
    }
  }

  @Test(timeout = 60000)
  public void testErrorHanding() throws InterruptedException{
    String msg = "ERROR YAR";
    SomeException expectedException = new SomeException(msg);
    sourceRunner.pipelineError = expectedException;
    sourceRunner.start();
    Utils.checkState(ThreadUtil.sleep(100), "Interrupted while sleeping");
    try {
      queue.putData(Arrays.asList(1));
    } catch (SomeException ex) {
      Assert.assertSame(ex, expectedException);
    }
  }

  @Test(timeout = 60000)
  public void testPeriodicEmptyBatches() throws InterruptedException{
    sourceRunner.start();
    for (int i = 0; i < 10; i++) {
      queue.putData(Arrays.asList(1));
      // let empty batch go through
      Utils.checkState(ThreadUtil.sleep(MAX_WAIT_TIME*2), "Interrupted while sleeping");
      queue.putData(Arrays.asList(1));
    }
    sourceRunner.stop = true;
    Utils.checkState(ThreadUtil.sleep(MAX_WAIT_TIME*2), "Interrupted while sleeping");
    Assert.assertEquals("20", source.getLastCommittedOffset());
    Assert.assertEquals(20, source.getRecordsProduced());
  }

  @Test(timeout = 60000)
  public void testNoEmptyBatches() throws InterruptedException{
    sourceRunner.start();
    for (int i = 0; i < 10; i++) {
      queue.putData(Arrays.asList(1));
      queue.putData(Arrays.asList(1));
    }
    sourceRunner.stop = true;
    Utils.checkState(ThreadUtil.sleep(MAX_WAIT_TIME*2), "Interrupted while sleeping");
    Assert.assertEquals("20", source.getLastCommittedOffset());
    Assert.assertEquals(20, source.getRecordsProduced());
  }

  @Test(timeout = 60000)
  public void testRandomEmptyBatches() throws InterruptedException{
    long seed = System.currentTimeMillis();
    LOG.info("Random seed: " + seed);
    Random random = new Random(seed);
    sourceRunner.start();
    int expectedRecords = 0;
    for (int i = 0; i < 50; i++) {
      if (random.nextBoolean()) {
        queue.putData(Arrays.asList(1));
        expectedRecords++;
      } else {
        Utils.checkState(ThreadUtil.sleep(MAX_WAIT_TIME*2), "Interrupted while sleeping");
      }
    }
    sourceRunner.stop = true;
    Utils.checkState(ThreadUtil.sleep(MAX_WAIT_TIME*2), "Interrupted while sleeping");
    Assert.assertEquals(String.valueOf(expectedRecords), source.getLastCommittedOffset());
    Assert.assertEquals(expectedRecords, source.getRecordsProduced());
  }

  private static class MockSourceRunner extends Thread {
    ClusterQueueConsumer source;
    Throwable capturedThrowable;
    /**
     * Simulates an error thrown during pipeline execution
     */
    Throwable pipelineError;
    volatile boolean stop;
    volatile int iterations;

    MockSourceRunner(ClusterQueueConsumer source) {
      this.source = source;
      this.iterations = 0;
    }

    public void run() {
      try {
        for (; !stop; iterations++) {
          if (pipelineError != null) {
            source.errorNotification(pipelineError);
          }
          OffsetAndResult offsetAndResult = source.produce(MAX_WAIT_TIME);
          source.commit(offsetAndResult.getOffset());
        }
      } catch (Throwable throwable) {
        this.capturedThrowable = throwable;
        if (throwable instanceof InterruptedException) {
          LOG.info("Interrupted while getting data from queue: " + throwable, throwable);
        } else {
          LOG.error("Error in MockSource: " + throwable, throwable);
        }
      }
    }
  }
}
