/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin;

import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

@GenerateResourceBundle
@StageDef(
  version = "1.0.0",
  label = "Spark Streaming",
  description = "Ingests data from Spark Streaming",
  icon = "")
public class SparkStreamingSource extends BaseSource implements OffsetCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(SparkStreamingSource.class);
  private final SynchronousQueue<Object> queue = new SynchronousQueue<>();
  private int recordsProduced = 0;

  @Override
  public void destroy() {

  }

  @Override
  public void init() throws StageException {
    super.init();
  }

  @Override
  public void commit(String offset) throws StageException {
    try {
      LOG.debug("In commit hook ");
      queue.offer(offset, 5, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
  public void put(List<String> batch) throws InterruptedException {
    LOG.debug("Adding batch ");
    queue.put(batch);
    Object result = queue.poll(5, TimeUnit.MINUTES);
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

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    Throwable error = null;
    try {
      Object object;
      if ((object = queue.poll(10, TimeUnit.SECONDS)) != null) {
        List<String> batch = null;
        if (object instanceof List) {
          batch = (List)object;
        } else {
          throw new IllegalStateException("Producer expects List, got " + object.getClass().getSimpleName());
        }
        for (String line : batch) {
          LOG.debug("Got line " + line);
          Record record = getContext().createRecord("spark-streaming");
          Map<String, Field> map = new HashMap<>();
          map.put("text", Field.create(line));
          record.set(Field.create(map));
          batchMaker.addRecord(record);
          recordsProduced++;
        }
      } else {
        throw new IllegalStateException("Cannot poll from queue. This should never happen");
      }
    } catch (Throwable throwable) {
      error = throwable;
    } finally {
      if (error != null) {
        try {
          queue.offer(error, 5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        if (error instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
      }
    }
    if (error != null) {
      Throwables.propagate(error);
    }
    return lastSourceOffset;
  }

  public long getRecordsProduced() {
    return recordsProduced;
  }
}
