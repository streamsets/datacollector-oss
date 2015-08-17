/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.jms;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.config.DataFormat;

@GenerateResourceBundle
public enum JmsGroups implements Label {
  JMS("JMS"),
  CREDENTIALS("Credentials"),
  TEXT(DataFormat.TEXT.getLabel()),
  JSON(DataFormat.JSON.getLabel()),
  DELIMITED(DataFormat.DELIMITED.getLabel()),
  XML(DataFormat.XML.getLabel()),
  LOG(DataFormat.LOG.getLabel()),
  AVRO(DataFormat.AVRO.getLabel()),
  ;

  private final String label;

  JmsGroups(String label) {
    this.label = label;
  }

  public String getLabel() {
    return this.label;
  }
}
