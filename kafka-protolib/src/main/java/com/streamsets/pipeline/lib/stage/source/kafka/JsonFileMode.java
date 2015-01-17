/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;
import com.streamsets.pipeline.lib.json.StreamingJsonParser;

public enum JsonFileMode implements BaseEnumChooserValues.EnumWithLabel {
  ARRAY_OBJECTS("A file has an JSON array of objects", StreamingJsonParser.Mode.ARRAY_OBJECTS),
  MULTIPLE_OBJECTS("A file has multiple JSON objects", StreamingJsonParser.Mode.MULTIPLE_OBJECTS),
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
