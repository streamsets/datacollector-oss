package com.streamsets.pipeline.config;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by harikiran on 10/20/14.
 */
public class ModuleRegistry {

  private List<StaticModuleConfiguration> staticModuleConfigurations = null;

  public ModuleRegistry() {
    this.staticModuleConfigurations = new ArrayList<StaticModuleConfiguration>();
  }

  public List<StaticModuleConfiguration> getStaticModuleConfigurations() {
    return staticModuleConfigurations;
  }
}
