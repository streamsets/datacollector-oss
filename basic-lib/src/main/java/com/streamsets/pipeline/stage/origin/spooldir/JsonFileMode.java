/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.lib.json.StreamingJsonParser;

public enum JsonFileMode implements Label {
  ARRAY_OBJECTS("Array of objects", StreamingJsonParser.Mode.ARRAY_OBJECTS),
  MULTIPLE_OBJECTS("Multiple objects", StreamingJsonParser.Mode.MULTIPLE_OBJECTS),
  ;

  private final String label;
  private final StreamingJsonParser.Mode format;

  JsonFileMode(String label, StreamingJsonParser.Mode format) {
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
