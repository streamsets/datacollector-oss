/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.cluster;

import com.streamsets.pipeline.OffsetAndResult;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Implements a uni-directional data flow from producer to consumer.
 */
public class DataChannel {

  private final BlockingQueue<OffsetAndResult<Map.Entry>> dataQueue = new ArrayBlockingQueue<>(1);

  public boolean offer(OffsetAndResult<Map.Entry> batch, long timeout, TimeUnit unit) throws InterruptedException {
    return dataQueue.offer(batch, timeout, unit);
  }

  public OffsetAndResult<Map.Entry> take(long timeout, TimeUnit unit) throws InterruptedException {
    return dataQueue.poll(timeout, unit);
  }
}
