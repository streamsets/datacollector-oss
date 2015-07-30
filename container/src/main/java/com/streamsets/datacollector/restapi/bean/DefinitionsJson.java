/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi.bean;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefinitionsJson {
  List<PipelineDefinitionJson> pipeline;
  List<StageDefinitionJson> stages;
  Map<String, Object> rulesElMetadata;
  Map<String, Object> elCatalog;
  Set<Object> runtimeConfigs;

  public List<PipelineDefinitionJson> getPipeline() {
    return pipeline;
  }

  public void setPipeline(List<PipelineDefinitionJson> pipeline) {
    this.pipeline = pipeline;
  }

  public List<StageDefinitionJson> getStages() {
    return stages;
  }

  public void setStages(List<StageDefinitionJson> stages) {
    this.stages = stages;
  }

  public Map<String, Object> getRulesElMetadata() {
    return rulesElMetadata;
  }

  public void setRulesElMetadata(Map<String, Object> rulesElMetadata) {
    this.rulesElMetadata = rulesElMetadata;
  }

  public Map<String, Object> getElCatalog() {
    return elCatalog;
  }

  public void setElCatalog(Map<String, Object> elCatalog) {
    this.elCatalog = elCatalog;
  }

  public Set<Object> getRuntimeConfigs() {
    return runtimeConfigs;
  }

  public void setRuntimeConfigs(Set<Object> runtimeConfigs) {
    this.runtimeConfigs = runtimeConfigs;
  }
}
