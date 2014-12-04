/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.testharness;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.runner.StageContext;
import com.streamsets.pipeline.sdk.testharness.internal.Constants;
import com.streamsets.pipeline.sdk.testharness.internal.StageInfo;
import com.streamsets.pipeline.sdk.testharness.internal.BatchBuilder;
import com.streamsets.pipeline.sdk.testharness.internal.BatchMakerImpl;
import com.streamsets.pipeline.sdk.testharness.internal.StageBuilder;
import com.streamsets.pipeline.sdk.util.StageHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProcessorRunner<T extends Processor> {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessorRunner.class);

  private final T processor;
  private final BatchBuilder batchBuilder;
  private final BatchMaker batchMaker;
  private final Stage.Info info;
  private final Stage.Context context;

  /*******************************************************/
  /***************** public methods **********************/
  /*******************************************************/

  public Map<String, List<Record>> run() throws StageException {
    init();
    process();
    destroy();
    return ((BatchMakerImpl)batchMaker).getLaneToRecordsMap();
  }

  public void init() throws StageException {
    try {
      processor.init(info, (Processor.Context)context);
    } catch (StageException e) {
      LOG.error("Failed to init Processor. Message : " + e.getMessage());
      throw e;
    }
  }

  public void process() throws StageException {
    try {
      processor.process(batchBuilder.build(), batchMaker);
    } catch (StageException e) {
      LOG.error("Failed to process. Message : " + e.getMessage());
      throw e;
    }
  }

  public void destroy() {
    processor.destroy();
  }

  /*******************************************************/
  /***************** Builder Class ***********************/
  /*******************************************************/

  public static class Builder<T extends Processor> extends StageBuilder {

    private final BatchBuilder batchBuilder;
    private Set<String> outputLanes;

    public Builder(RecordProducer recordProducer) {
      this.batchBuilder = new BatchBuilder(recordProducer);
    }

    public Builder<T> addProcessor(Class<T> klass) {
      try {
        this.stage = klass.newInstance();
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
      return this;
    }

    public Builder<T> addProcessor(T processor) {
      this.stage = processor;
      return this;
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

    /**
     * Returns a {@link com.streamsets.pipeline.sdk.testharness.ProcessorRunner}
     * instance based on the configuration.
     *
     * @return
     * @throws com.streamsets.pipeline.api.StageException
     */
    public ProcessorRunner build() throws StageException {

      //validate that all required options are set
      if (!validateProcessor()) {
        throw new IllegalStateException(
          "SourceBuilder is not configured correctly. Please check the logs for errors.");
      }

      //configure the stage
      configureStage();

      //extract name and version of the stage from the stage def annotation
      StageDef stageDefAnnot = stage.getClass().getAnnotation(StageDef.class);
      info = new StageInfo(StageHelper.getStageNameFromClassName(stage.getClass().getName()), stageDefAnnot.version(),
          instanceName);
      //mockInfoAndContextForStage and stub Source.Context
      context = new StageContext(instanceName, outputLanes);

      //update batchbuilder
      batchBuilder.setSourceOffset(sourceOffset);
      batchBuilder.setMaxBatchSize(maxBatchSize);
      return new ProcessorRunner(
        (T) stage, batchBuilder, outputLanes, info, context);
    }

    private boolean validateProcessor() {
      //validate general configuration
      boolean valid = validateStage();
      //validate specific configuration
      if (outputLanes == null || outputLanes.isEmpty()) {
        LOG.info("The 'outputLanes' is not set. Generating a single lane 'lane'.");
        if (outputLanes == null) {
          outputLanes = new HashSet<String>();
        }
        outputLanes.add(Constants.DEFAULT_LANE);
      }
      return valid;
    }
  }

  /*******************************************************/
  /***************** private methods *********************/
  /*******************************************************/

  private ProcessorRunner(T processor, BatchBuilder batchBuilder,
                          Set<String> outputlanes, Stage.Info info, Stage.Context context) {
    this.processor = processor;
    this.batchBuilder = batchBuilder;
    this.batchMaker = new BatchMakerImpl(outputlanes);
    this.info = info;
    this.context = context;
  }

}
