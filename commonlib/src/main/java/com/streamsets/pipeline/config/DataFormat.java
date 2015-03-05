/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.lib.generator.CharDataGeneratorFactory;
import com.streamsets.pipeline.lib.parser.CharDataParserFactory;

public enum DataFormat implements Label {
  TEXT("Text", CharDataParserFactory.Format.TEXT, CharDataGeneratorFactory.Format.TEXT),
  JSON("JSON", CharDataParserFactory.Format.JSON, CharDataGeneratorFactory.Format.JSON),
  DELIMITED("Delimited", CharDataParserFactory.Format.DELIMITED, CharDataGeneratorFactory.Format.DELIMITED),
  XML("XML", CharDataParserFactory.Format.XML, CharDataGeneratorFactory.Format.XML),
  SDC_JSON("SDC Record", CharDataParserFactory.Format.SDC_RECORD, CharDataGeneratorFactory.Format.SDC_RECORD),

  ;

  private final String label;
  private final CharDataParserFactory.Format parserFormat;
  private final CharDataGeneratorFactory.Format generatorFormat;

  DataFormat(String label, CharDataParserFactory.Format parserFormat, CharDataGeneratorFactory.Format generatorFormat) {
    this.label = label;
    this.parserFormat = parserFormat;
    this.generatorFormat = generatorFormat;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public CharDataParserFactory.Format getParserFormat() {
    return parserFormat;
  }

  public CharDataGeneratorFactory.Format getGeneratorFormat() {
    return generatorFormat;
  }

}
