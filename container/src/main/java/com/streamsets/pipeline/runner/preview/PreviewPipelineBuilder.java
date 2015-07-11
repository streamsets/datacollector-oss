/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.preview;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.runner.PipelineRunner;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.ValidationUtil;
import com.streamsets.pipeline.validation.Issue;
import com.streamsets.pipeline.validation.Issues;
import com.streamsets.pipeline.validation.PipelineConfigurationValidator;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.runner.Pipeline;
import com.streamsets.pipeline.runner.PipelineRuntimeException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.UUID;

public class PreviewPipelineBuilder {

  @SuppressWarnings("unchecked")
  private StageConfiguration createPlugStage(List<String> lanes) {
    StageConfiguration stageConf = new StageConfiguration(PreviewStageLibraryTask.NAME + UUID.randomUUID().toString(),
                                                          PreviewStageLibraryTask.LIBRARY, PreviewStageLibraryTask.NAME,
                                                          PreviewStageLibraryTask.VERSION, Collections.EMPTY_LIST,
                                                          Collections.EMPTY_MAP, lanes, Collections.EMPTY_LIST);
    stageConf.setSystemGenerated();
    return stageConf;
  }

  private final StageLibraryTask stageLib;
  private final String name;
  private final PipelineConfiguration pipelineConf;
  private final String endStageInstanceName;

  /**
   * Constructor
   *
   * @param stageLib Stage Library Task
   * @param name Name of pipeline
   * @param pipelineConf Pipeline Configuration
   * @param endStageInstanceName Optional parameter, if passed builder will generate a partial pipeline and
   *                             endStage is exclusive
   */
  public PreviewPipelineBuilder(StageLibraryTask stageLib, String name, PipelineConfiguration pipelineConf,
                                String endStageInstanceName) {
    this.stageLib = new PreviewStageLibraryTask(stageLib);
    this.name = name;
    this.pipelineConf = pipelineConf;
    this.endStageInstanceName = endStageInstanceName;
  }

  public PreviewPipeline build(PipelineRunner runner) throws PipelineRuntimeException, StageException {
    if(endStageInstanceName != null && endStageInstanceName.trim().length() > 0) {
      List<StageConfiguration> stages = new ArrayList<>();
      Set<String> allowedOutputLanes = new HashSet<>();
      for(StageConfiguration stageConfiguration: pipelineConf.getStages()) {
        if (stageConfiguration.getInstanceName().equals(endStageInstanceName)) {
          allowedOutputLanes.addAll(stageConfiguration.getInputLanes());
          break;
        }
        stages.add(stageConfiguration);
      }

      ListIterator li = stages.listIterator(stages.size());

      while(li.hasPrevious()) {
        StageConfiguration stageConfiguration = (StageConfiguration) li.previous();
        boolean contains = false;
        List<String> outputLanes = stageConfiguration.getOutputLanes();
        for(String outputLane :outputLanes) {
          if(allowedOutputLanes.contains(outputLane)){
            contains = true;
            break;
          }
        }

        if(contains) {
          allowedOutputLanes.addAll(stageConfiguration.getInputLanes());
        } else {
          li.remove();
        }
      }

      pipelineConf.setStages(stages);
    }

    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLib, name, pipelineConf);
    if (validator.validate() || validator.canPreview()) {
      List<String> openLanes = validator.getOpenLanes();
      if (!openLanes.isEmpty()) {
        pipelineConf.getStages().add(createPlugStage(openLanes));
      }
    } else {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0154, ValidationUtil.getFirstIssueAsString(name,
        validator.getIssues()));
    }
     Pipeline.Builder builder = new Pipeline.Builder(stageLib, name + ":preview", pipelineConf);
     Pipeline pipeline = builder.build(runner);
     if (pipeline != null) {
       List<Issue> configIssues = pipeline.validateConfigs();
       if (!configIssues.isEmpty()) {
         Issues issues = new Issues(configIssues);
         throw new PipelineRuntimeException(issues);
       }
       return new PreviewPipeline(pipeline, validator.getIssues());
     } else {
       Issues issues = new Issues(builder.getIssues());
       throw new PipelineRuntimeException(issues);
    }
  }

}
