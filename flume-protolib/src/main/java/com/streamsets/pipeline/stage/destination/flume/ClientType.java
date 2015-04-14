/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.flume;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum ClientType implements Label {
  AVRO_FAILOVER("Avro Failover"),
  AVRO_LOAD_BALANCING("Avro Load Balancing"),
  THRIFT("Thrift"),

  ;

  private final String label;

  ClientType(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

}