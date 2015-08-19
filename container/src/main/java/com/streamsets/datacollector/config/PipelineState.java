/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.config;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

import java.io.Serializable;

@GenerateResourceBundle
public enum PipelineState implements Label, Serializable {
  RUNNING("Running"),
  START_ERROR("Start Error"),
  RUN_ERROR("Run Error"),
  STOPPED("Stopped"),
  FINISHED("Finished")

  ;

  private final String label;

  PipelineState(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

}
