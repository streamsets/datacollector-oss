/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.devnull;

import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;

@StageDef(
    version = 1,
    label = "Discard",
    description = "Discards records",
    icon=""
)
@HideConfigs(preconditions = true, onErrorRecord = true)
@ErrorStage
@GenerateResourceBundle
public class ToErrorNullDTarget extends NullDTarget {
}
