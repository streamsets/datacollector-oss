package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by harikiran on 10/20/14.
 */
public class RuntimePipelineConfiguration {

  private List<RuntimeStageConfiguration> runtimeModuleConfigurations = null;
  private String uuid = null;
  private Map<String, List<String>> errorsMap = null;


  public RuntimePipelineConfiguration() {
    runtimeModuleConfigurations = new ArrayList<RuntimeStageConfiguration>();
    errorsMap = new LinkedHashMap<String, List<String>>();
  }

  @JsonFormat()
  public List<RuntimeStageConfiguration> getRuntimeModuleConfigurations() {
    return runtimeModuleConfigurations;
  }

  public void setRuntimeModuleConfigurations(List<RuntimeStageConfiguration> runtimeModuleConfigurations) {
    this.runtimeModuleConfigurations = runtimeModuleConfigurations;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public Map<String, List<String>> getErrorsMap() {
    return errorsMap;
  }

  public void setErrorsMap(Map<String, List<String>> errorsMap) {
    this.errorsMap = errorsMap;
  }
}
