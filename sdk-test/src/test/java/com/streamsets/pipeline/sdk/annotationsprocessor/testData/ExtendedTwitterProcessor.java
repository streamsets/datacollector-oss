/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.annotationsprocessor.testData;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfig;
import com.streamsets.pipeline.api.StageDef;

@GenerateResourceBundle
@StageDef(description = "processes twitter feeds", label = "extended_twitter_processor"
  , version = 1)
@HideConfig(value = {"tweetType1", "tweetType2"})
public class ExtendedTwitterProcessor extends TwitterProcessor {
}
