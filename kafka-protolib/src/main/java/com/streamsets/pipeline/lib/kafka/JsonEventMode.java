/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.lib.json.StreamingJsonParser;

public enum JsonEventMode implements Label {
  ARRAY_OBJECTS("An event has an JSON array of objects", StreamingJsonParser.Mode.ARRAY_OBJECTS),
  MULTIPLE_OBJECTS("An event has one or multiple JSON objects", StreamingJsonParser.Mode.MULTIPLE_OBJECTS),
  ;

  private final String label;
  private final StreamingJsonParser.Mode format;

  JsonEventMode(String label, StreamingJsonParser.Mode format) {
    this.label = label;
    this.format = format;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public StreamingJsonParser.Mode getFormat() {
    return format;
  }

}
