/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.lib.parser.CharDataParserFactory;

public enum DataFormat implements Label {
  TEXT("Text", CharDataParserFactory.Format.TEXT),
  JSON("JSON", CharDataParserFactory.Format.JSON),
  DELIMITED("Delimited", CharDataParserFactory.Format.DELIMITED),
  XML("XML", CharDataParserFactory.Format.XML),
  SDC_JSON("SDC Record", CharDataParserFactory.Format.SDC_RECORD),

  ;

  private final String label;
  private final CharDataParserFactory.Format format;

  DataFormat(String label, CharDataParserFactory.Format format) {
    this.label = label;
    this.format = format;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public CharDataParserFactory.Format getFormat() {
    return format;
  }

}
