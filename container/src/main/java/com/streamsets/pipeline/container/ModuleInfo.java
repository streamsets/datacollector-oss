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
package com.streamsets.pipeline.container;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Module;

public class ModuleInfo implements Module.Info {
  private String name;
  private String version;
  private String description;
  private String instanceName;

  private static boolean validName(String name) {
    if (name.isEmpty()) {
      return false;
    }
    if (name.startsWith(ContainerConstants.INVALID_INSTANCE_NAME_PREFIX)) {
      return false;
    }
    for (char c : ContainerConstants.INVALID_MODULE_CHARACTERS.toCharArray()) {
      if (name.indexOf(c) > -1) {
        return false;
      }
    }
    return true;
  }

  private static String invalidNameMessage() {
    return "cannot be null, cannot be empty, cannot start with '" + ContainerConstants.INVALID_INSTANCE_NAME_PREFIX +
           "' and cannot contain any of the following characters '" + ContainerConstants.INVALID_MODULE_CHARACTERS + "'";
  }

  public ModuleInfo(String name, String version, String description, String instanceName) {
    this(name, version, description, instanceName, true);
  }

  public ModuleInfo(String name, String version, String description, String instanceName, boolean validateArgs) {
    if (validateArgs) {
      Preconditions.checkNotNull(name, "name cannot be null");
      Preconditions.checkNotNull(version, "version cannot be null");
      Preconditions.checkNotNull(description, "description cannot be null");
      Preconditions.checkNotNull(instanceName, "instanceName cannot be null");
      Preconditions.checkArgument(validName(name), String.format("Invalid name '%s', %s", name, invalidNameMessage()));
      Preconditions.checkArgument(validName(instanceName), String.format("Invalid instanceName '%s', %s", instanceName,
                                                                         invalidNameMessage()));
    }
    this.name = name;
    this.version = version;
    this.description = description;
    this.instanceName = instanceName;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getVersion() {
    return version;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public String getInstanceName() {
    return instanceName;
  }

}
