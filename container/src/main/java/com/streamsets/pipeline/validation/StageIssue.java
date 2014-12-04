/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.validation;

import com.streamsets.pipeline.container.Utils;

public class StageIssue extends Issue {
  private final String instanceName;
  private final String configName;

  public static StageIssue createStageIssue(String instanceName, ValidationError error, Object... args) {
    return new StageIssue(instanceName, null, error, args);
  }

  public static StageIssue createConfigIssue(String instanceName, String configName, ValidationError error,
      Object... args) {
    return new StageIssue(instanceName, configName, error, args);
  }

  private StageIssue(String instanceName, String configName, ValidationError error, Object... args) {
    super(error, args);
    this.instanceName = instanceName;
    this.configName = configName;
  }

  public String getInstanceName() {
    return instanceName;
  }

  public String getConfigName() {
    return configName;
  }

  public String getLevel() {
    return (configName == null) ? "STAGE" : "STAGE_CONFIG";
  }

  public String toString() {
    return (configName == null)
           ? Utils.format("Instance '{}': {}", getInstanceName(), super.toString())
           : Utils.format("Instance '{}' config '{}': {}", getInstanceName(), getConfigName(), super.toString());
  }
}
