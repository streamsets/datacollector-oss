/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */

package com.streamsets.dc.datacollector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.runner.BatchListener;
import com.streamsets.pipeline.runner.Pipeline;


public class EmbeddedDataCollectorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedDataCollectorFactory.class);
  public static Source startPipeline(final Runnable postBatchRunnable) throws Exception {
    EmbeddedDataCollector embeddedDataCollector = new EmbeddedDataCollector();
    embeddedDataCollector.init();
    embeddedDataCollector.startPipeline();
    long startTime = System.currentTimeMillis();
    long endTime = startTime;
    long diff = endTime - startTime;
    while (embeddedDataCollector.getPipeline() == null && diff < 60000) {
      LOG.debug("Waiting for pipeline to be created");
      Thread.sleep(100);
      endTime = System.currentTimeMillis();
      diff = endTime - startTime;
    }
    if (diff > 60000) {
      throw new IllegalStateException(Utils.format("Pipeline has not started even after waiting '{}'", diff));
    }
    Pipeline realPipeline = embeddedDataCollector.getPipeline();
    realPipeline.getRunner().registerListener(new BatchListener() {
      @Override
      public void preBatch() {
        // nothing
      }

      @Override
      public void postBatch() {
        postBatchRunnable.run();
      }
    });
    return realPipeline.getSource();
  }
}
