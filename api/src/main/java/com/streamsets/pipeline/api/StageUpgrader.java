/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
