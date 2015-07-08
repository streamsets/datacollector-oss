/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.hadoop;

import com.streamsets.pipeline.BootstrapCluster;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class PipelineMapper extends Mapper {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineMapper.class);

  @Override
  protected void setup(Mapper.Context context) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void map(Object key, Object value, Mapper.Context context)
    throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void run(Mapper.Context context) throws IOException, InterruptedException {
    Integer id = context.getTaskAttemptID().getTaskID().getId();
    Properties properties;
    Method mapFunction;
    Method destroyFunction;
    try {
      properties = BootstrapCluster.getProperties();
      mapFunction = BootstrapCluster.getHadoopMapFunction();
      destroyFunction = BootstrapCluster.getHadoopDestroyFunction();
    } catch (Exception ex) {
      if (ex instanceof RuntimeException) {
        throw (RuntimeException)ex;
      } else if (ex instanceof IOException) {
        throw (IOException)ex;
      } else if (ex instanceof InterruptedException) {
        throw (InterruptedException)ex;
      } else {
        throw new RuntimeException("Error initializing: " + ex, ex);
      }
    }
    int batchSize = Integer.parseInt(properties.getProperty("production.maxBatchSize", "1000").trim());
    boolean errorOccurred = true;
    try {
      List<MapEntry> batch = new ArrayList<>();
      boolean hasNext = context.nextKeyValue();
      while (hasNext) {
        while (hasNext && batch.size() < batchSize) {
          batch.add(new MapEntry(String.valueOf(context.getCurrentKey()), String.valueOf(context.getCurrentValue())));
          hasNext = context.nextKeyValue(); // not like iterator.hasNext, actually advances
        }
        mapFunction.invoke(null, properties, id, batch);
        batch.clear();
      }
      errorOccurred = false;
    } catch (IllegalAccessException ex) {
      throw new RuntimeException("Error invoking map function: " + ex, ex);
    } catch (InvocationTargetException ex) {
      Throwable error = ex;
      if (error.getCause() != null) {
        error = ex.getCause();
      }
      throw new RuntimeException("Error invoking map function: " + error, error);
    } finally {
      try {
        destroyFunction.invoke(null);
      } catch (Throwable throwable) {
        LOG.warn("Error on destroy: {}", throwable, throwable);
        if (!errorOccurred) {
          if (throwable instanceof RuntimeException) {
            throw (RuntimeException)throwable;
          } else if (throwable instanceof Error) {
            throw (Error)throwable;
          } else {
            throw new RuntimeException(throwable);
          }
        }
      }
    }
  }

  private static class MapEntry implements Map.Entry {
    private final Object key;
    private final Object value;

    private MapEntry(Object key, Object value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public Object getKey() {
      return key;
    }

    @Override
    public Object getValue() {
      return value;
    }

    @Override
    public Object setValue(Object value) {
      throw new UnsupportedOperationException();
    }
  }
}
