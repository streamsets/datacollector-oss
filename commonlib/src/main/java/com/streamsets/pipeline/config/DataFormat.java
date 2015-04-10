/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.lib.generator.CharDataGeneratorFactory;
import com.streamsets.pipeline.lib.parser.DataParserFormat;

@GenerateResourceBundle
public enum DataFormat implements Label {
  TEXT("Text", DataParserFormat.TEXT, CharDataGeneratorFactory.Format.TEXT),
  JSON("JSON", DataParserFormat.JSON, CharDataGeneratorFactory.Format.JSON),
  DELIMITED("Delimited", DataParserFormat.DELIMITED, CharDataGeneratorFactory.Format.DELIMITED),
  XML("XML", DataParserFormat.XML, CharDataGeneratorFactory.Format.XML),
  SDC_JSON("SDC Record", DataParserFormat.SDC_RECORD, CharDataGeneratorFactory.Format.SDC_RECORD),
  LOG("Log", DataParserFormat.LOG, CharDataGeneratorFactory.Format.LOG),


  ;

  private final String label;
  private final DataParserFormat parserFormat;
  private final CharDataGeneratorFactory.Format generatorFormat;

  DataFormat(String label, DataParserFormat parserFormat, CharDataGeneratorFactory.Format generatorFormat) {
    this.label = label;
    this.parserFormat = parserFormat;
    this.generatorFormat = generatorFormat;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public DataParserFormat getParserFormat() {
    return parserFormat;
  }

  public CharDataGeneratorFactory.Format getGeneratorFormat() {
    return generatorFormat;
  }

}
