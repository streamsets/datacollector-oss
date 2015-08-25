/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.sdcipc;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.configurablestage.DTarget;

@StageDef(
    version = 1,
    label = "RPC",
    description = "Sends records via RPC to a Data Collector pipeline that uses an RPC origin",
    icon="sdcipc.png"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class SdcIpcDTarget extends DTarget {

  @ConfigDefBean
  public Configs config;

  @Override
  protected Target createTarget() {
    return new SdcIpcTarget(config);
  }
}
