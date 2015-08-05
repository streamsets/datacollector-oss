/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.runner.common;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.Observer;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.production.ProductionSourceOffsetCommitterOffsetTracker;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.ValidationUtil;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.datacollector.validation.PipelineConfigurationValidator;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.StageException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;

import java.util.List;

public class ProductionPipelineBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(ProductionPipelineBuilder.class);
  private static final String PRODUCTION_PIPELINE_SUFFIX = ":production";

  private final StageLibraryTask stageLib;
  private final String name;
  private final String rev;
  private final RuntimeInfo runtimeInfo;

  private ProductionPipelineRunner runner;
  private Observer observer;

  public ProductionPipelineBuilder(@Named("name") String name, @Named("rev") String rev, RuntimeInfo runtimeInfo,
                                   StageLibraryTask stageLib, ProductionPipelineRunner runner, Observer observer) {
    this.name = name;
    this.rev = rev;
    this.runtimeInfo = runtimeInfo;
    this.stageLib = stageLib;
    this.runner = runner;
    this.observer = observer;
  }

  public ProductionPipeline build(PipelineConfiguration pipelineConf)
    throws PipelineRuntimeException, StageException {
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLib, name, pipelineConf);
    pipelineConf = validator.validate();
    if (validator.getIssues().hasIssues()) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0158, ValidationUtil.getFirstIssueAsString(name,
        validator.getIssues()));
    }
    Pipeline pipeline = new Pipeline.Builder(stageLib, name + PRODUCTION_PIPELINE_SUFFIX, name, rev, pipelineConf)
      .setObserver(observer).build(runner);

    if (pipeline.getSource() instanceof OffsetCommitter) {
      runner.setOffsetTracker(new ProductionSourceOffsetCommitterOffsetTracker(name, rev, runtimeInfo,
        (OffsetCommitter) pipeline.getSource()));
    }
    return new ProductionPipeline(name, rev, runtimeInfo, pipelineConf, pipeline);
  }

}
