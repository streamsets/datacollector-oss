/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.sdk.testharness;

import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.sdk.testharness.internal.Constants;
import com.streamsets.pipeline.sdk.testharness.internal.StageInfo;
import com.streamsets.pipeline.sdk.testharness.internal.BatchMakerImpl;
import com.streamsets.pipeline.sdk.testharness.internal.SourceContextImpl;
import com.streamsets.pipeline.sdk.testharness.internal.StageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SourceRunner <T extends Source> {

  private static final Logger LOG = LoggerFactory.getLogger(SourceRunner.class);

  private final T source;
  private final int maxBatchSize;
  private final String sourceOffset;
  private final BatchMaker batchMaker;
  private final Stage.Info info;
  private final Stage.Context context;

  /*******************************************************/
  /***************** public methods **********************/
  /*******************************************************/

  public Map<String, List<Record>> run() throws StageException {
    try {
      source.init(info, (Source.Context)context);
    } catch (StageException e) {
      LOG.error("Failed to init Source. Message : " + e.getMessage());
      throw e;
    }

    try {
      source.produce(sourceOffset, maxBatchSize, batchMaker);
    } catch (StageException e) {
      LOG.error("Failed to produce. Message : " + e.getMessage());
      throw e;
    }

    source.destroy();
    return ((BatchMakerImpl)batchMaker).getLaneToRecordsMap();
  }

  /*******************************************************/
  /***************** Builder Class ***********************/
  /*******************************************************/

  public static class Builder<T extends Source> extends StageBuilder {

    /*The output lanes into which records will be produced*/
    private Set<String> outputLanes = null;

    public Builder() {

    }

    public Builder<T> sourceOffset(String sourceOffset) {
      this.sourceOffset = sourceOffset;
      return this;
    }

    public Builder<T> maxBatchSize(int maxBatchSize) {
      this.maxBatchSize = maxBatchSize;
      return this;
    }

    public Builder<T> outputLanes(Set<String> outputLanes) {
      this.outputLanes = outputLanes;
      return this;
    }

    public Builder<T> addSource(T source, String instanceName) {
      this.stage = source;
      this.instanceName = instanceName;
      return this;
    }

    public Builder<T> addSource(Class<T> klass) {
      try {
        this.stage = klass.newInstance();
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
      return this;
    }

    public Builder<T> configure(String key, Object value) {
      configMap.put(key, value);
      return this;
    }

    public SourceRunner<T> build() throws StageException {

      //validate that all required options are set
      if(!validateSource()) {
        throw new IllegalStateException(
          "SourceBuilder is not configured correctly. Please check the logs for errors.");
      }

      //configure the stage
      configureStage();

      //extract name and version of the stage from the stage def annotation
      StageDef stageDefAnnot = stage.getClass().getAnnotation(StageDef.class);
      info = new StageInfo(
        stageDefAnnot.name(), stageDefAnnot.version(), instanceName);
      //mockInfoAndContextForStage and stub Source.Context
      context = new SourceContextImpl(instanceName, outputLanes);

      return new SourceRunner<T>(
        (T)stage, maxBatchSize, sourceOffset, outputLanes, info, context);
    }

    private boolean validateSource() {
      //validate general configuration
      boolean valid = validateStage();
      //validate specific configuration
      if(outputLanes == null || outputLanes.isEmpty()) {
        LOG.info("The 'outputLanes' is not set. Generating a single lane 'lane'.");
        if(outputLanes == null) {
          outputLanes = new HashSet<String>();
        }
        outputLanes.add(Constants.DEFAULT_LANE);
      }
      return valid;
    }
  }

  /*******************************************************/
  /***************** private methods **********************/
  /*******************************************************/

  private SourceRunner(T source,
                       int maxBatchSize,
                       String sourceOffset, Set<String> outputLanes,
                       Stage.Info info,
                       Stage.Context context) {
    this.maxBatchSize = maxBatchSize;
    this.source = source;
    this.sourceOffset = sourceOffset;
    this.batchMaker = new BatchMakerImpl(outputLanes);
    this.info = info;
    this.context = context;
  }

}
