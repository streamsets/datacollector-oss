/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.streamsets.pipeline.api.Label;

public enum ProducerPayloadType implements Label {
  SDC_RECORDS("SDC Record"),
  JSON("JSON"),
  CSV("Delimited"),
  TEXT("Text"),

  ;
  private final String label;

  ProducerPayloadType(String label) {
    this.label = label;
  }


  @Override
  public String getLabel() {
    return label;
  }
}
