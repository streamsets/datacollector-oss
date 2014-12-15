/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.annotationsprocessor.testData;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.ChooserValues;

import java.util.List;

public class TweetTypeProvider implements ChooserValues{
  @Override
  public List<String> getValues() {
    return ImmutableList.of("NEWS", "TECH", "SOCIAL");
  }

  @Override
  public List<String> getLabels() {
    return ImmutableList.of("NEWS", "TECH", "SOCIAL");
  }
}
