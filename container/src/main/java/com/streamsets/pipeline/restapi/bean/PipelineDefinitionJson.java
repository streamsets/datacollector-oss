/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import java.util.List;

public class PipelineDefinitionJson {

  private final com.streamsets.pipeline.config.PipelineDefinition pipelineDefinition;

  public PipelineDefinitionJson(com.streamsets.pipeline.config.PipelineDefinition pipelineDefinition) {
    this.pipelineDefinition = pipelineDefinition;
  }

  public List<ConfigDefinitionJson> getConfigDefinitions() {
    return BeanHelper.wrapConfigDefinitions(pipelineDefinition.getConfigDefinitions());
  }

  public ConfigGroupDefinitionJson getConfigGroupDefinition() {
    return BeanHelper.wrapConfigGroupDefinition(pipelineDefinition.getConfigGroupDefinition());
  }

}