/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.sdcipc;

import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;

@StageDef(
    version = 1,
    label = "Write to Another Pipeline",
    description = "",
    icon = "")
@ErrorStage
@HideConfigs(preconditions = true, onErrorRecord = true)
@GenerateResourceBundle
public class ToErrorSdcIpcDTarget extends SdcIpcDTarget {

}
