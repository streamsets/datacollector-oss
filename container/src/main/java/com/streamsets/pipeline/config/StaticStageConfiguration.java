package com.streamsets.pipeline.config;

import java.util.List;

/**
 * Captures the configuration options for a {@link com.streamsets.pipeline.api.Stage}.
 *
 */
public class StaticStageConfiguration {

  private final String name;
  private final String version;
  private final String shortDescription;
  private final String description;
  private final StageType moduleType;
  private List<ConfigOption> configOptionList;

  public StaticStageConfiguration(String name, String version, String shortDescription,
                                  String description, StageType moduleType, List<ConfigOption> configOptionList) {
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

  public StageType getModuleType() {
    return moduleType;
  }

  public List<ConfigOption> getConfigOptionList() {
    return configOptionList;
  }

  public void setConfigOptionList(List<ConfigOption> configOptionList) {
    this.configOptionList = configOptionList;
  }
}
