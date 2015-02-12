/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.validation;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.impl.Utils;

public class StageIssue extends Issue {
  private final String instanceName;
  private final boolean errorStage;

  protected StageIssue(boolean errorStage, String instanceName, ErrorCode error, Object... args) {
    this(errorStage, instanceName, null, null, error, args);
  }

  protected StageIssue(boolean errorStage, String instanceName, String configGroup, String configName,
      ErrorCode error, Object... args) {
    super(configName, configGroup, error, args);
    this.errorStage = errorStage;
    this.instanceName = instanceName;
  }

  public String getInstanceName() {
    return instanceName;
  }

  public boolean isErrorStage() {
    return errorStage;
  }

  public String getLevel() {
    return (getConfigName() == null) ? "STAGE" : "STAGE_CONFIG";
  }

  public String toString() {
    return (getConfigName() == null)
           ? Utils.format("Instance '{}' errorStage '{}': {}", getInstanceName(), isErrorStage(), super.toString())
           : Utils.format("Instance '{}' errorStage '{}' config '{}': {}", getInstanceName(), isErrorStage(),
                          getConfigName(), super.toString());
  }
}
