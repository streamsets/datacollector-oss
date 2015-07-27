/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.creation;

import java.util.List;

public class PipelineBean {
  private final PipelineConfigBean config;
  private final List<StageBean> stages;
  private final StageBean errorStage;

  public PipelineBean(PipelineConfigBean config, List<StageBean> stages, StageBean errorStage) {
    this.config = config;
    this.stages = stages;
    this.errorStage = errorStage;
  }

  public PipelineConfigBean getConfig() {
    return config;
  }

  public List<StageBean> getStages() {
    return stages;
  }

  public StageBean getErrorStage() {
    return errorStage;
  }

}
