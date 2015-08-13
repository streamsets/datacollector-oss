/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.sdcipc;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DSourceOffsetCommitter;

@StageDef(
    version = 1,
    label = "Pipeline Origin",
    description = "Receives records from a SDC pipeline using a Pipeline Destination",
    icon="sdcipc.png"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class SdcIpcDSource extends DSourceOffsetCommitter {

  @ConfigDefBean
  public Configs configs;

  @Override
  protected Source createSource() {
    return new SdcIpcSource(configs);
  }
}
