/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api;

import java.util.List;

public interface StageUpgrader {

  @GenerateResourceBundle
  public enum Error implements ErrorCode {
    UPGRADER_00("Upgrader not implemented for stage '{}:{}' instance '{}'"),
    UPGRADER_01("Cannot upgrade stage '{}:{}' instance '{}' from version '{}' to version '{}'"),

    ;


    private final String message;

    Error(String message) {
      this.message = message;
    }

    @Override
    public String getCode() {
      return name();
    }

    @Override
    public String getMessage() {
      return message;
    }
  }

  public static class Default implements StageUpgrader {

    @Override
    public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion,
        List<Config> configs) throws
        StageException {
      throw new StageException(Error.UPGRADER_00, library, stageName, stageInstance);
    }
  };

  public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion,
      List<Config> configs) throws StageException;

}
