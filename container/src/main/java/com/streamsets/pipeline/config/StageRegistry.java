package com.streamsets.pipeline.config;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.streamsets.pipeline.serde.StageConfigurationSerializer;

import java.util.ArrayList;
import java.util.List;

/**
 * A registry of all {@link com.streamsets.pipeline.api.Stage} objects
 * available in an installation.
 */
@JsonSerialize(using = StageConfigurationSerializer.class)
public class StageRegistry {

  private List<StaticStageConfiguration> staticStageConfigurations = null;

  public StageRegistry() {
    this.staticStageConfigurations = new ArrayList<StaticStageConfiguration>();
  }

  public List<StaticStageConfiguration> getStaticStageConfigurations() {
    return staticStageConfigurations;
  }
}
