package com.streamsets.pipeline.sdk;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by harikiran on 10/20/14.
 */
public class StageCollection {

  private List<StageConfiguration> stageConfigurations = null;

  public StageCollection() {
    this.stageConfigurations = new ArrayList<StageConfiguration>();
  }

  public List<StageConfiguration> getStageConfigurations() {
    return stageConfigurations;
  }
}
