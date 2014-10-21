package com.streamsets.pipeline.config;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.streamsets.pipeline.serde.ModuleConfigurationSerializer;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by harikiran on 10/20/14.
 */
@JsonSerialize(using = ModuleConfigurationSerializer.class)
public class ModuleRegistry {

  private List<StaticModuleConfiguration> staticModuleConfigurations = null;

  public ModuleRegistry() {
    this.staticModuleConfigurations = new ArrayList<StaticModuleConfiguration>();
  }

  public List<StaticModuleConfiguration> getStaticModuleConfigurations() {
    return staticModuleConfigurations;
  }
}
