package com.streamsets.pipeline.config;

import java.util.List;

/**
 * Created by harikiran on 10/20/14.
 */

public class StaticModuleConfiguration {

  private final String name;
  private final String version;
  private final String shortDescription;
  private final String description;
  private final ModuleType moduleType;
  private List<ConfigOption> configOptionList;

  public StaticModuleConfiguration(String name, String version, String shortDescription,
                                   String description, ModuleType moduleType, List<ConfigOption> configOptionList) {
    this.name = name;
    this.version = version;
    this.shortDescription = shortDescription;
    this.description = description;
    this.moduleType = moduleType;
    this.configOptionList = configOptionList;
  }

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
  }

  public String getShortDescription() {
    return shortDescription;
  }

  public String getDescription() {
    return description;
  }

  public ModuleType getModuleType() {
    return moduleType;
  }

  public List<ConfigOption> getConfigOptionList() {
    return configOptionList;
  }

  public void setConfigOptionList(List<ConfigOption> configOptionList) {
    this.configOptionList = configOptionList;
  }
}
