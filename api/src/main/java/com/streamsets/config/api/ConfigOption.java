package com.streamsets.config.api;

/**
 * Created by harikiran on 10/18/14.
 */
public class ConfigOption {

  private final String name;
  private final ConfigType type;
  private final String shortDescription;
  private final String description;
  private final String defaultValue;
  private final boolean mandatory;

  public ConfigOption(String name, ConfigType type, String shortDescription
    , String description, String defaultValue, boolean mandatory) {
    this.name = name;
    this.type = type;
    this.shortDescription = shortDescription;
    this.description = description;
    this.defaultValue = defaultValue;
    this.mandatory = mandatory;
  }

  public String getName() {
    return name;
  }

  public ConfigType getType() {
    return type;
  }

  public String getShortDescription() {
    return shortDescription;
  }

  public String getDescription() {
    return description;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public boolean isMandatory() {
    return mandatory;
  }
}
