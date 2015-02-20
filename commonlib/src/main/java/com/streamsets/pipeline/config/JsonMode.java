/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.lib.json.StreamingJsonParser;

public enum JsonMode implements Label {
  ARRAY_OBJECTS("JSON array of objects", StreamingJsonParser.Mode.ARRAY_OBJECTS),
  MULTIPLE_OBJECTS("Multiple JSON objects", StreamingJsonParser.Mode.MULTIPLE_OBJECTS),
  ;

  private final String label;
  private final StreamingJsonParser.Mode format;

  JsonMode(String label, StreamingJsonParser.Mode format) {
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
