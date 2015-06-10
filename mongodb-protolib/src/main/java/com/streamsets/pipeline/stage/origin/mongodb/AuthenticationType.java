/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.mongodb;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum AuthenticationType implements Label {
  NONE("None"),
  USER_PASS("Username/Password"),
  X509("X.509"),
  ;

  private final String label;

  AuthenticationType(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
