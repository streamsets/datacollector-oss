/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.validation;

public abstract class StageIssueCreator {

  public abstract  StageIssue createStageIssue(String instanceName, ValidationError error, Object... args);

  public abstract StageIssue createConfigIssue(String instanceName, String configGroup, String configName,
      ValidationError error, Object... args);

  public static StageIssueCreator getStageCreator() {
    return new StageIssueCreator() {
      @Override
      public StageIssue createStageIssue(String instanceName, ValidationError error, Object... args) {
        return new StageIssue(false, instanceName, null, null, error, args);
      }

      @Override
      public StageIssue createConfigIssue(String instanceName, String configGroup, String configName,
          ValidationError error, Object... args) {
        return new StageIssue(false, instanceName, configGroup, configName, error, args);
      }
    };
  }

  public static StageIssueCreator getErrorStageCreator() {
    return new StageIssueCreator() {
      @Override
      public StageIssue createStageIssue(String instanceName, ValidationError error, Object... args) {
        return new StageIssue(true, instanceName, null, null, error, args);
      }

      @Override
      public StageIssue createConfigIssue(String instanceName, String configGroup, String configName,
          ValidationError error, Object... args) {
        return new StageIssue(true, instanceName, configGroup, configName, error, args);
      }
    };
  }

}
