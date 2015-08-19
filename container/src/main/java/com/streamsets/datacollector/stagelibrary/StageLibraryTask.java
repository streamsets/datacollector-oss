/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.stagelibrary;

import com.streamsets.datacollector.config.PipelineDefinition;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.task.Task;

import java.util.List;
import java.util.Map;

public interface StageLibraryTask extends Task, ClassLoaderReleaser {

  public static final String STAGES_DEFINITION_RESOURCE = "PipelineStages.json";

  public static final String EL_DEFINITION_RESOURCE = "ElDefinitions.json";

  public PipelineDefinition getPipeline();

  public List<StageDefinition> getStages();

  public StageDefinition getStage(String library, String name, boolean forExecution);

  public Map<String, String> getLibraryNameAliases();

}
