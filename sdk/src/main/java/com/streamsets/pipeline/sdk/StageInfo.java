/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk;

import com.streamsets.pipeline.api.Stage;

class StageInfo implements Stage.Info {
  private final String name;
  private final String version;
  private final String instanceName;

  public StageInfo(String name, String version, String instanceName) {
    this.name = name;
    this.version = version;
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
  public String getInstanceName() {
    return instanceName;
  }

}
