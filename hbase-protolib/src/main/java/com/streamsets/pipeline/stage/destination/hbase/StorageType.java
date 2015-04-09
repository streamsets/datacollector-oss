/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in
 * whole or part without written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hbase;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum StorageType implements Label {
  TEXT("Text"), BINARY("Binary"), JSON_STRING("JSON String");

  private String label;

  StorageType(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

}
