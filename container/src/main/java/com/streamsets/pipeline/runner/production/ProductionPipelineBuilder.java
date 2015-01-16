/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.runner.Pipeline;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.validation.Issues;
import com.streamsets.pipeline.validation.PipelineConfigurationValidator;
import com.streamsets.pipeline.validation.StageIssue;

import java.util.List;
import java.util.Map;

public class ProductionPipelineBuilder {

  private static final String PRODUCTION_PIPELINE_SUFFIX = ":production";

  private final StageLibraryTask stageLib;
  private final String name;
  private final PipelineConfiguration pipelineConf;

  public ProductionPipelineBuilder(StageLibraryTask stageLib, String name, PipelineConfiguration pipelineConf) {
    this.stageLib = stageLib;
    this.name = name;
    this.pipelineConf = pipelineConf;
  }

  public ProductionPipeline build(ProductionPipelineRunner runner, SourceOffsetTracker offsetTracker)
      throws PipelineRuntimeException {
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLib, name, pipelineConf);
    if (!validator.validate()) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0158, getFirstIssueAsString(
          validator.getIssues()));
    }
    Pipeline pipeline = new Pipeline.Builder(stageLib, name + PRODUCTION_PIPELINE_SUFFIX, pipelineConf).build(runner);
    if (pipeline.getSource() instanceof OffsetCommitter) {
      runner.setOffsetTracker(new ProductionSourceOffsetCommitterOffsetTracker((OffsetCommitter) pipeline.getSource()));
    } else {
      runner.setOffsetTracker(offsetTracker);
    }
    return new ProductionPipeline(pipeline);
  }

  private String getFirstIssueAsString(Issues issues) {
    StringBuilder sb = new StringBuilder();
    if(issues.getPipelineIssues().size() > 0) {
      sb.append("[").append(name).append("] ").append(issues.getPipelineIssues().get(0).getMessage());
    } else if (issues.getStageIssues().entrySet().size() > 0) {
      Map.Entry<String, List<StageIssue>> e = issues.getStageIssues().entrySet().iterator().next();
      sb.append("[").append(e.getKey()).append("] ").append(e.getValue().get(0).getMessage());
    }
    sb.append("...");
    return sb.toString();
  }

}
