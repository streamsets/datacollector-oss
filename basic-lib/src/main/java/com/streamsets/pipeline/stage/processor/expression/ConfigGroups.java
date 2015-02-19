/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.expression;

import com.streamsets.pipeline.api.Label;

public enum ConfigGroups implements Label {
  EXPRESSIONS;

  @Override
  public String getLabel() {
    return "Expressions";
  }

}
