/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin;

import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SparkStreamingTextSource extends SparkStreamingSource {
  private static final Logger LOG = LoggerFactory.getLogger(SparkStreamingTextSource.class);
  private int recordsProduced = 0;

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    Throwable error = null;
    try {
      Object object;
      // TODO implement periodic timeout if spark does not send empty batches
      if ((object = getElement(10000)) != null) {
        LOG.info("Batch = " + object);
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
        LOG.debug("Didn't get any data, must be empty RDD");
      }
    } catch (Throwable throwable) {
      error = throwable;
    } finally {
      if (error != null) {
        try {
          putElement(error);
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

  @Override
  public long getRecordsProduced() {
    return recordsProduced;
  }
}
