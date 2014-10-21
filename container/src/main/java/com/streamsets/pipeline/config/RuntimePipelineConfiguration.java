package com.streamsets.pipeline.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.streamsets.pipeline.serde.RuntimePipelineConfigDeserializer;
import com.streamsets.pipeline.serde.RuntimePipelineConfigSerializer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by harikiran on 10/20/14.
 */
@JsonSerialize(using = RuntimePipelineConfigSerializer.class)
@JsonDeserialize(using = RuntimePipelineConfigDeserializer.class)
public class RuntimePipelineConfiguration {

  private List<RuntimeModuleConfiguration> runtimeModuleConfigurations = null;
  private String uuid = null;
  private Map<String, List<String>> errorsMap = null;


  public RuntimePipelineConfiguration() {
    runtimeModuleConfigurations = new ArrayList<RuntimeModuleConfiguration>();
    errorsMap = new LinkedHashMap<String, List<String>>();
  }

  public List<RuntimeModuleConfiguration> getRuntimeModuleConfigurations() {
    return runtimeModuleConfigurations;
  }

  public void setRuntimeModuleConfigurations(List<RuntimeModuleConfiguration> runtimeModuleConfigurations) {
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
