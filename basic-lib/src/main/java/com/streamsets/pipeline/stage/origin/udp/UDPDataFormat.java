/*
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.udp;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

/**
 * This is just a dummy enum until the udp source supports
 * other data formats.
 */
@GenerateResourceBundle
public enum UDPDataFormat implements Label {
  NETFLOW("NetFlow"),
  SYSLOG("Syslog")
  ;

  private final String label;

  UDPDataFormat(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
