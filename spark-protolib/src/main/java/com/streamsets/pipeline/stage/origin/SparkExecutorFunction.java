/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin;

import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;


/**
 * This function is serialized and pushed over the write to all executors
 */
public class SparkExecutorFunction implements VoidFunction<Iterator<String>>, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SparkExecutorFunction.class);
  private static final Object poolCreationLock = new Object();
  private static volatile EmbeddedSDCPool sdcPool;
  private Properties properties;
  private String pipelineJson;

  public SparkExecutorFunction(Properties properties, String pipelineJson) {
    this.properties = properties;
    this.pipelineJson = pipelineJson;
  }

  private void initialize() throws Exception {
    synchronized (poolCreationLock) {
      if (sdcPool == null) {
        sdcPool = new EmbeddedSDCPool(properties, pipelineJson);
      }
    }
  }

  @Override
  public void call(Iterator<String> stringIterator) throws Exception {
    initialize();
    EmbeddedSDC embeddedSDC = sdcPool.getEmbeddedSDC();
    LOG.info("Embedded sdc: " + embeddedSDC);
    List<String> batch = new ArrayList<>();
    while (stringIterator.hasNext()) {
      String msg = stringIterator.next();
      batch.add(msg);
      LOG.info("Got message: " + msg);
    }
    embeddedSDC.getSource().put(batch);
  }

  public static void execute(Properties properties, String pipelineJson, Iterator<String> stringIterator)
  throws Exception {
    SparkExecutorFunction function = new SparkExecutorFunction(properties, pipelineJson);
    function.call(stringIterator);
  }

  public static long getRecordsProducedJVMWide() {
    try {
      long result = 0;
      if (sdcPool == null) {
        throw new RuntimeException("Embedded SDC pool is not initialized");
      }
      for (EmbeddedSDC sdc : sdcPool.getTotalInstances()) {
        result += sdc.getSource().getRecordsProduced();
      }
      return result;
    } catch (RuntimeException ex) {
      LOG.error("Error in getRecordsProducedJVMWide: " + ex, ex);
      throw ex;
    }
  }
}
