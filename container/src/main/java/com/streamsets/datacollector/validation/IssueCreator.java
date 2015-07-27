/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.validation;

import com.streamsets.pipeline.api.ErrorCode;

public abstract class IssueCreator {

  public abstract Issue create(ErrorCode error, Object... args);

  public abstract Issue create(String configGroup, ErrorCode error, Object... args);

  public abstract Issue create(String configGroup, String configName, ErrorCode error, Object... args);

  private IssueCreator() {
  }

  public static IssueCreator getPipeline() {
    return new IssueCreator() {
      @Override
      public Issue create(ErrorCode error, Object... args) {
        return new Issue(null, null, null, error, args);
      }

      @Override
      public Issue create(String configGroup, String configName, ErrorCode error, Object... args) {
        return new Issue(null, configGroup, configName, error, args);
      }

      @Override
      public Issue create(String configGroup, ErrorCode error, Object... args) {
        return new Issue(null, configGroup, null, error, args);
      }

    };
  }

  public static IssueCreator getStage(final String instanceName) {
    return new IssueCreator() {
      @Override
      public Issue create(ErrorCode error, Object... args) {
        return new Issue(instanceName, null, null, error, args);
      }

      @Override
      public Issue create(String configGroup, String configName, ErrorCode error, Object... args) {
        return new Issue(instanceName, configGroup, configName, error, args);
      }

      @Override
      public Issue create(String configGroup, ErrorCode error, Object... args) {
        return new Issue(instanceName, configGroup, null, error, args);
      }

    };
  }


}
