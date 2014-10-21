package com.streamsets.pipeline.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.streamsets.pipeline.serde.RuntimePipelineConfigDeserializer;
import com.streamsets.pipeline.serde.RuntimePipelineConfigSerializer;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by harikiran on 10/20/14.
 */
@JsonSerialize(using = RuntimePipelineConfigSerializer.class)
@JsonDeserialize(using = RuntimePipelineConfigDeserializer.class)
public class RuntimePipelineConfiguration {

  private List<RuntimeModuleConfiguration> runtimeModuleConfigurations = null;

  //TODO<Hari>: Model error message

  //TODO<Hari>: model a version attribute to check for

  public RuntimePipelineConfiguration() {
    runtimeModuleConfigurations = new ArrayList<RuntimeModuleConfiguration>();
  }

  public List<RuntimeModuleConfiguration> getRuntimeModuleConfigurations() {
    return runtimeModuleConfigurations;
  }

}
