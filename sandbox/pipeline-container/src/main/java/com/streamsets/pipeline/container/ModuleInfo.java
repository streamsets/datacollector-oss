/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
    for (String invalidName : ContainerConstants.INVALID_MODULE_NAMES) {
      if (name.equals(invalidName)) {
        return  false;
      }
    }
    for (char c : ContainerConstants.INVALID_MODULE_CHARACTERS.toCharArray()) {
      if (name.indexOf(c) > -1) {
        return false;
      }
    }
    return true;
  }

  private static String invalidNameMessage() {
    return "cannot be null, cannot be empty, cannot be " + ContainerConstants.INVALID_MODULE_NAMES +
           " and cannot contain any of the following characters '" + ContainerConstants.INVALID_MODULE_CHARACTERS + "'";
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
