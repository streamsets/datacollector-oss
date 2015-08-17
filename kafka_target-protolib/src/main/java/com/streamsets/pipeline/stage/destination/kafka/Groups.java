/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.kafka;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum Groups implements Label {
  KAFKA("Kafka"),
  TEXT("Text"),
  JSON("JSON"),
  DELIMITED("Delimited"),
  AVRO("Avro"),
  BINARY("Binary"),
  ;

  private final String label;

  private Groups(String label) {
    this.label = label;
  }

  public String getLabel() {
    return this.label;
  }
}