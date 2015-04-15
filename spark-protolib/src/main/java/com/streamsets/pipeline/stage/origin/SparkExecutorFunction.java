/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin;

import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.runner.Pipeline;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * This function is serailized and pushed over the write to all executors
 */
public class SparkExecutorFunction implements VoidFunction<Iterator<String>>, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SparkExecutorFunction.class);
  private final EmbeddedSDCConf sdcConf;
  private static volatile EmbeddedSDCPool sdcPool;

  public SparkExecutorFunction(EmbeddedSDCConf sdcConf) {
    this.sdcConf = sdcConf;
  }

  public EmbeddedSDCPool createPoolInstance(int min, EmbeddedSDCConf conf) throws Exception {
    if (sdcPool == null) {
      synchronized (this) {
        if (sdcPool == null) {
          sdcPool = new EmbeddedSDCPool(min, conf);
        }
      }
    }
    return sdcPool;
  }

  @Override
  public void call(Iterator<String> stringIterator) throws Exception {
    System.setProperty(RuntimeInfo.TRANSIENT_ENVIRONMENT, "true");
    //Make this configurable later on
    createPoolInstance(1, sdcConf);
    EmbeddedSDC embeddedSDC = sdcPool.getEmbeddedSDC(sdcConf);
    LOG.info("Embedded sdc" + embeddedSDC);
    List<String> batch = new ArrayList<>();
    while (stringIterator.hasNext()) {
      String msg = stringIterator.next();
      batch.add(msg);
      LOG.info("Got message: " + msg);
    }
    ((SparkStreamingSource) embeddedSDC.getPipeline().getSource()).put(batch);
  }

  public static long getRecordsProducedJVMWide() {
    long result = 0;
    if (sdcPool == null) {
      throw new RuntimeException("Embedded SDC pool is not initialized");
    }
    for (EmbeddedSDC sdc : sdcPool.getTotalInstances()) {
      result += ((SparkStreamingSource) sdc.getPipeline().getSource()).getRecordsProduced();
    }
    return result;
  }
}
