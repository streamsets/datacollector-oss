/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka;

import com.streamsets.pipeline.api.Label;

public enum Groups implements Label {
    KAFKA("KAFKA"),
    JSON("JSON"),
    DELIMITED("Delimited"),
    XML("XML"),

    ;

    private final String label;

    private Groups(String label) {
      this.label = label;
    }

    public String getLabel() {
      return this.label;
    }
  }

