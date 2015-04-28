/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spark;

import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.StageException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

/**
 * Maintains a synchronous queue to which spark transformation function writes batch of RDD's
 * The pipeline thread will consume from this queue and do the processing of the batch
 *
 */
public class SparkStreamingQueue {
  private static final Logger LOG = LoggerFactory.getLogger(SparkStreamingQueue.class);
  private final SynchronousQueue<Object> queue = new SynchronousQueue<>();

  public void commit(String offset) throws StageException {
    try {
      LOG.debug("In commit hook ");
      queue.put(offset);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  protected <T> void put(List<T> batch) throws InterruptedException {
    LOG.debug("Adding batch ");
    queue.put(batch);
    // TODO - poll for a configurable timeout
    Object result = queue.take();
    if (result == null) {
      throw new IllegalStateException("Timed out waiting for response");
    } else if (result instanceof InterruptedException) {
      throw (InterruptedException)result;
    } else if (result instanceof Throwable) {
      Throwables.propagate((Throwable)result);
    } else {
      // this means success
    }
  }

  protected Object getElement(int timeout) throws InterruptedException {
    return queue.poll(timeout, TimeUnit.MILLISECONDS);
  }

  protected void putElement(Object object) throws InterruptedException {
    queue.put(object);
  }

}
