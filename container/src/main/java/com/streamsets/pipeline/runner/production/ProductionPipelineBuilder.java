/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.runner.Observer;
import com.streamsets.pipeline.runner.Pipeline;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.ValidationUtil;
import com.streamsets.pipeline.validation.Issues;
import com.streamsets.pipeline.validation.PipelineConfigurationValidator;
import com.streamsets.pipeline.validation.StageIssue;

import java.util.List;

public class ProductionPipelineBuilder {

  private static final String PRODUCTION_PIPELINE_SUFFIX = ":production";

  private final StageLibraryTask stageLib;
  private final String name;
  private final String rev;
  private final PipelineConfiguration pipelineConf;
  private final RuntimeInfo runtimeInfo;

  public ProductionPipelineBuilder(StageLibraryTask stageLib, String name, String rev, RuntimeInfo runtimeInfo,
                                   PipelineConfiguration pipelineConf) {
    this.stageLib = stageLib;
    this.name = name;
    this.rev = rev;
    this.runtimeInfo = runtimeInfo;
    this.pipelineConf = pipelineConf;
  }

  public ProductionPipeline build(ProductionPipelineRunner runner, SourceOffsetTracker offsetTracker, Observer observer)
      throws PipelineRuntimeException, StageException {
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLib, name, pipelineConf, true);
    if (!validator.validate()) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0158, ValidationUtil.getFirstIssueAsString(name,
        validator.getIssues()));
    }
    Pipeline pipeline = new Pipeline.Builder(stageLib, name + PRODUCTION_PIPELINE_SUFFIX, pipelineConf)
      .setObserver(observer).build(runner);

    List<StageIssue> configIssues = pipeline.validateConfigs();
    if (!configIssues.isEmpty()) {
      Issues issues = new Issues(configIssues);
      throw new PipelineRuntimeException(issues);
    }
    if (pipeline.getSource() instanceof OffsetCommitter) {
      runner.setOffsetTracker(new ProductionSourceOffsetCommitterOffsetTracker(name, rev, runtimeInfo,
        (OffsetCommitter) pipeline.getSource()));
    } else {
      runner.setOffsetTracker(offsetTracker);
    }
    return new ProductionPipeline(runtimeInfo, pipelineConf, pipeline);
  }

}
