package com.streamsets.pipeline.sdk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by harikiran on 10/23/14.
 */
public class StageConfiguration {

  Map<String, String> stageOptions = null;
  List<Map<String, String>> configOptions = null;

  public StageConfiguration() {
    this.stageOptions = new HashMap<String, String>();
    this.configOptions = new ArrayList<Map<String, String>>();
  }

  public Map<String, String> getStageOptions() {
    return stageOptions;
  }

  public List<Map<String, String>> getConfigOptions() {
    return configOptions;
  }
}
