/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.toerror;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.configurablestage.DTarget;

@StageDef(
    version = 1,
    label = "To Error",
    description = "Sends records to the pipeline configured error records handling",
    icon="toerror.png"
)
@HideConfigs(preconditions = true, onErrorRecord = true)
@GenerateResourceBundle
public class ToErrorDTarget extends DTarget {

  @Override
  protected Target createTarget() {
    return new ToErrorTarget();
  }
}
