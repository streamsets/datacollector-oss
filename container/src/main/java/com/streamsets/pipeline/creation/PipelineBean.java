/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.creation;

import java.util.List;

public class PipelineBean {
  private final PipelineConfigBean config;
  private final List<StageBean> stages;

  public PipelineBean(PipelineConfigBean config, List<StageBean> stages) {
    this.config = config;
    this.stages = stages;
  }

  public PipelineConfigBean getConfig() {
    return config;
  }

  public List<StageBean> getStages() {
    return stages;
  }

}
