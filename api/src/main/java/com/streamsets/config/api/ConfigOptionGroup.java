package com.streamsets.config.api;

import java.util.List;

/**
 * Created by harikiran on 10/18/14.
 */
public class ConfigOptionGroup {

  private final String name;
  private List<ConfigOption> configOptions = null;

  public ConfigOptionGroup(String name, List<ConfigOption> configOptions) {
    this.name = name;
    this.configOptions = configOptions;
  }

  public String getName() {
    return name;
  }

  public List<ConfigOption> getConfigOptions() {
    return configOptions;
  }

}
