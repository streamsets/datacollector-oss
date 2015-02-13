/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.streamsets.pipeline.api.Label;

public enum ConsumerPayloadType implements Label {
  LOG("Text"),
  JSON("JSON"),
  CSV("Delimited"),
  XML("XML"),
  SDC_RECORDS("SDC Record"),

  ;

  private String label;

  ConsumerPayloadType(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }


}
