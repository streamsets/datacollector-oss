/*
 * Copyright 2019 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.upgrader;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  YAML_UPGRADER_01("Configuration '{}' could not parse value as JSON, error: {}"),
  YAML_UPGRADER_02("Configuration '{}' could not find in name pattern '{}' regex group '{}'"),
  YAML_UPGRADER_03("Configuration '{}' not found"),
  YAML_UPGRADER_04("Configuration '{}' has JSON data, replaceConfigValue ifOldValueMatches cannot be used"),
  YAML_UPGRADER_05("Configuration '{}' has JSON data, cannot be used as old value token replacement"),
  YAML_UPGRADER_06("Configuration '{}' could not be converted to List<Config>: {}"),
  YAML_UPGRADER_07("Could not get '{}' Yaml upgrader: {}"),
  YAML_UPGRADER_08("Invalid action in '{}' toVersion upgrader configuration for '{}' stage from '{}' "),
  YAML_UPGRADER_09("Invalid action in '{}' configuration for '{}' stage from '{}' "),
  YAML_UPGRADER_10("Invalid upgrader version '{}'"),
  YAML_UPGRADER_11("Nested iterations are not supported in '{}' toVersion upgrader configuration for '{}' stage from '{}' "),
  YAML_UPGRADER_12("Configuration '{}' EL '{}' evaluation error: {}"),
  YAML_UPGRADER_13("Cannot find service definition with name '{}' "),
  YAML_UPGRADER_14("Cannot set match index to non-integer value of '{}"),
  YAML_UPGRADER_15("Cannot set both newValue and newValueFromMatchIndex in same upgrader"),
  YAML_UPGRADER_16("At least one of newValue and newValueFromMatchIndex must be set"),
  ;

  private final String message;

  Errors(String message) {
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
