/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
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

/**
 * This function is serailized and pushed over the write to all executors
 */
public class SparkExecutorFunction implements VoidFunction<Iterator<String>>, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SparkExecutorFunction.class);
  private final EmbeddedSDCConf sdcConf;
  private EmbeddedSDC embeddedSDC;

  public SparkExecutorFunction(EmbeddedSDCConf sdcConf) {
    this.sdcConf = sdcConf;
  }
  @Override
  public void call(Iterator<String> stringIterator) throws Exception {
    if (embeddedSDC == null) {
      embeddedSDC = new EmbeddedSDC(sdcConf);
      embeddedSDC.init();
    }
    List<String> batch = new ArrayList<>();
    while (stringIterator.hasNext()) {
      String msg = stringIterator.next();
      batch.add(msg);
      LOG.info("Got message: " + msg);
    }
    embeddedSDC.put(batch);
  }
}
