/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka.cluster;

import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.StageException;

import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Maintains a synchronous queue to which spark transformation function writes batch of RDD's
 * The pipeline thread will consume from this queue and do the processing of the batch
 *
 */
public class ClusterQueue {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterQueue.class);
  private static final boolean IS_TRACE_ENABLED = LOG.isTraceEnabled();
  private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger();
  private final SynchronousQueue<Object> queue = new SynchronousQueue<>();
  private final int instanceId;
  private Throwable pipelineError;

  public ClusterQueue() {
    instanceId = INSTANCE_COUNTER.incrementAndGet();
  }

  public void commitData(String offset) throws StageException {
    try {
      if(IS_TRACE_ENABLED) {
        LOG.trace("{}: {}: commitData, offset {}", instanceId, Thread.currentThread().getName(), offset);
      }
      queue.put(offset);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public <T> void putData(List<T> batch) throws InterruptedException {
    if(IS_TRACE_ENABLED) {
      LOG.trace("{}: {}: putData batch of size: {}", instanceId, Thread.currentThread().getName(), batch.size());
    }
    // check for an error before we call put
    // should not occur unless the source.produce
    // throws an error before getData is called
    // which is a small window
    Throwable error;
    error = pipelineError;
    if (error != null) {
      throw Throwables.propagate(error);
    }
    queue.put(batch);
    // re-check for an error state after we call put since
    // put is a blocking operation, since the pipeline
    // takes some time to run, most errors will be caught
    // in the block below where we poll from the queue
    error = pipelineError;
    if (error != null) {
      throw Throwables.propagate(error);
    }
    if (batch.isEmpty()) {
      LOG.debug("Received empty batch from spark");
    } else {
      // if we block here forever it's either a logic error on our part
      // or the pipeline died and will not return a value
      long start = System.currentTimeMillis();
      while (true) {
        Object result = queue.poll(5, TimeUnit.MINUTES);
        if (result == null) {
          long elapsedMinutes = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - start);
          LOG.warn(Utils.format("Have not received the result of the last batch after {} minutes", elapsedMinutes));
        } else if (result instanceof InterruptedException) {
          Thread.currentThread().interrupt();
          throw (InterruptedException)result;
        } else if (result instanceof Throwable) {
          Throwables.propagate((Throwable)result);
        } else {
          // this means success
          if(IS_TRACE_ENABLED) {
            LOG.trace("{}: {}: Put resulted in {}", instanceId, Thread.currentThread().getName(), result);
          }
          break;
        }
      }
    }
  }

  public Object getData(int timeout) throws InterruptedException {
    if(IS_TRACE_ENABLED) {
      LOG.trace("{}: {}: getData", instanceId, Thread.currentThread().getName());
    }
    return queue.poll(timeout, TimeUnit.MILLISECONDS);
  }

  public void putError(Throwable throwable) throws InterruptedException {
    if(IS_TRACE_ENABLED) {
      LOG.trace("{}: {}: putError", instanceId, Thread.currentThread().getName());
    }
    pipelineError = throwable;
    queue.put(throwable);
  }

  public boolean inErrorState() {
    return pipelineError != null;
  }

}
