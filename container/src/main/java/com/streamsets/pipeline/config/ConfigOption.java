package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

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
  private final String group;

  @JsonCreator
  public ConfigOption(
      @JsonProperty("name") String name,
      @JsonProperty("type") ConfigType type,
      @JsonProperty("shortDescription") String shortDescription,
      @JsonProperty("description") String description,
      @JsonProperty("defaultValue")  String defaultValue,
      @JsonProperty("mandatory")    boolean mandatory,
      @JsonProperty("group")  String group) {
    this.name = name;
    this.type = type;
    this.shortDescription = shortDescription;
    this.description = description;
    this.defaultValue = defaultValue;
    this.mandatory = mandatory;
    this.group = group;
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

  public String getGroup() { return group; }
}
