/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */

package com.streamsets.pipeline.datacollector;

import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.runner.BatchListener;
import com.streamsets.pipeline.runner.Pipeline;
import java.util.Properties;

public class EmbeddedDataCollectorFactory {

  public static Source createPipeline(Properties properties, String pipelineJson,
                                      final Runnable postBatchRunnable) throws Exception {
    EmbeddedDataCollector embeddedPipeline = new EmbeddedDataCollector();
    embeddedPipeline.init();
    embeddedPipeline.startPipeline(pipelineJson);
    Pipeline realPipeline = embeddedPipeline.getPipeline();
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
