/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.creation;

import com.streamsets.pipeline.api.Stage;

public class StageBean {
  private final StageConfigBean systemConfigs;
  private final Stage stage;

  public StageBean(StageConfigBean systemConfigs, Stage stage) {
    this.systemConfigs = systemConfigs;
    this.stage = stage;
  }

  public StageConfigBean getSystemConfigs() {
    return systemConfigs;
  }

  public Stage getStage() {
    return stage;
  }
}
