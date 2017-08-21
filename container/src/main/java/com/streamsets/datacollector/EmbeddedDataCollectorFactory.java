/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector;

import com.streamsets.datacollector.runner.BatchListener;
import com.streamsets.datacollector.runner.Pipe;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class EmbeddedDataCollectorFactory {
  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedDataCollectorFactory.class);
  public static final String SPARK_DPROCESSOR_CLASS =
      "com.streamsets.pipeline.stage.processor.spark.SparkDProcessor";

  private EmbeddedDataCollectorFactory() {}

  public static PipelineStartResult startPipeline(final Runnable postBatchRunnable) throws Exception {
    EmbeddedDataCollector embeddedDataCollector = new EmbeddedDataCollector();
    embeddedDataCollector.init();
    embeddedDataCollector.startPipeline();
    long startTime = System.currentTimeMillis();
    long endTime = startTime;
    long diff = 0;
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
    List<Object> sparkProcessors = new ArrayList<>();
    List<Pipe> pipes = realPipeline.getRunners().get(0).getPipes(); // Cluster pipelines are single threaded.
    for (Pipe pipe : pipes) {
      Stage stage = pipe.getStage().getStage();
      if (stage.getClass().getCanonicalName().equals(SPARK_DPROCESSOR_CLASS)) {
        LOG.info("Added Spark Processor for " + stage.toString());
        if (!sparkProcessors.contains(stage)) {
          sparkProcessors.add(stage);
        }
      }
    }
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
    return new PipelineStartResult(realPipeline.getSource(), sparkProcessors);
  }
}
