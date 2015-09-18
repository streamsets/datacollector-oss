/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streamsets.datacollector.runner.BatchListener;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.impl.Utils;


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
