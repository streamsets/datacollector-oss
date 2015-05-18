/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spark;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class SparkStreamingQueueConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(SparkStreamingQueueConsumer.class);
  private static final boolean IS_TRACE_ENABLED = LOG.isDebugEnabled();
  private SparkStreamingQueue queue;
  /**
   * The last committed offset is used to detect cases where we timeout waiting
   * for a patch and as such we should not call commit on the queue
   * as no-one is waiting for us. This variable should only be access
   * by the thread running the pipeline can take the following states:
   * <ul>
   * <li>null - an error occured in produce</li>
   * <li>empty string - first call to commit</li>
   * <li>the same offset as last run - empty batch</li>
   * <li>a new offset - a new batch, caller is waiting for result on work queue</li>
   * </ul>
   */
  private String lastCommittedOffset;
  private int recordsProduced;

  SparkStreamingQueueConsumer(SparkStreamingQueue queue) {
    this.queue = queue;
    this.recordsProduced = 0;
    this.lastCommittedOffset = "";
  }

  public <T> OffsetAndResult<T> produce(int maxWaitTime) {
    try {
      Object object = queue.getData(maxWaitTime);
      if (object == null) {
        if (IS_TRACE_ENABLED) {
          LOG.trace("Didn't get any data");
        }
        return new OffsetAndResult<T>(String.valueOf(recordsProduced), Collections.<T>emptyList());
      } else if (object instanceof List) {
        List result = (List)object;
        recordsProduced += result.size();
        return new OffsetAndResult<T>(String.valueOf(recordsProduced), result);
      }
      throw new IllegalStateException("Consumer expects List, got " + object.getClass().getSimpleName());
    } catch (Throwable throwable) {
      if (throwable instanceof InterruptedException) {
        LOG.info("Interrupted while getting data from queue: " + throwable, throwable);
      } else {
        LOG.error("Error in MockSource: " + throwable, throwable);
      }
      try {
        queue.putError(throwable);
      } catch (InterruptedException ex) {
        LOG.info("Interrupted while placing error on queue: " + ex, ex);
      }
      throw Throwables.propagate(throwable);
    }
  }

  public void commit(String offset) throws StageException {
    if (IS_TRACE_ENABLED) {
      LOG.trace("Last committed offset {}, attempting to commit {}", lastCommittedOffset, offset);
    }
    Utils.checkState(null != lastCommittedOffset, "Last committed offset is null, meaning an error occurred in produce");
    if (lastCommittedOffset.equals(offset)) {
      if (IS_TRACE_ENABLED) {
        LOG.trace("Offsets are equal, batch was empty");
      }
    } else if((lastCommittedOffset.isEmpty() && "0".equals(offset))) {
      if (IS_TRACE_ENABLED) {
        LOG.trace("First call to commit and batch is assumed to be empty");
      }
    } else {
      queue.commitData(offset);
      lastCommittedOffset = offset;
    }
  }

  public void errorNotification(Throwable throwable) {
    String msg = "Received error notification: " + throwable;
    LOG.error(msg, throwable);
    try {
      queue.putError(throwable);
    } catch (InterruptedException ex) {
      LOG.error("Interrupted while putting error on queue: " + throwable, ex);
      Thread.currentThread().interrupt();
    }
  }

  @VisibleForTesting
  public String getLastCommittedOffset() {
    return lastCommittedOffset;
  }

  public long getRecordsProduced() {
    return recordsProduced;
  }
}
