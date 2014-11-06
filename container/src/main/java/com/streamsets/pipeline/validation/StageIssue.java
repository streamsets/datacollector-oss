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
package com.streamsets.pipeline.validation;

public class StageIssue extends Issue {
  private final String instanceName;
  private final String configName;

  public static StageIssue createStageIssue(String instanceName, String bundleKey, String defaultTemplate,
      Object... args) {
    return new StageIssue(instanceName, null, bundleKey, defaultTemplate, args);
  }

  public static StageIssue createConfigIssue(String instanceName, String configName, String bundleKey,
      String defaultTemplate, Object... args) {
    return new StageIssue(instanceName, configName, bundleKey, defaultTemplate, args);
  }

  private StageIssue(String instanceName, String configName, String bundleKey, String defaultTemplate, Object... args) {
    super(bundleKey, defaultTemplate, args);
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
           ? String.format("Instance '%s': %s", getInstanceName(), super.toString())
           : String.format("Instance '%s' config '%s': %s", getInstanceName(), getConfigName(), super.toString());
  }
}
