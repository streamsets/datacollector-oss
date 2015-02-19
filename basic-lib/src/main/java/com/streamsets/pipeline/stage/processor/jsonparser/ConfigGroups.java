/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.jsonparser;

import com.streamsets.pipeline.api.Label;

public enum ConfigGroups implements Label {
  JSON;

  @Override
  public String getLabel() {
    return "Parse";
  }

}
