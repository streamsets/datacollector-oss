/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.lib.generator.DataGeneratorFormat;
import com.streamsets.pipeline.lib.parser.DataParserFormat;

@GenerateResourceBundle
public enum DataFormat implements Label {
  TEXT("Text", DataParserFormat.TEXT, DataGeneratorFormat.TEXT),
  JSON("JSON", DataParserFormat.JSON, DataGeneratorFormat.JSON),
  DELIMITED("Delimited", DataParserFormat.DELIMITED, DataGeneratorFormat.DELIMITED),
  XML("XML", DataParserFormat.XML, null),
  SDC_JSON("SDC Record", DataParserFormat.SDC_RECORD, DataGeneratorFormat.SDC_RECORD),
  LOG("Log", DataParserFormat.LOG, null),
  AVRO("Avro", DataParserFormat.AVRO, DataGeneratorFormat.AVRO),
  BINARY("Binary", DataParserFormat.BINARY, DataGeneratorFormat.BINARY),
  ;

  private final String label;
  private final DataParserFormat parserFormat;
  private final DataGeneratorFormat generatorFormat;

  DataFormat(String label, DataParserFormat parserFormat, DataGeneratorFormat generatorFormat) {
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

  public DataGeneratorFormat getGeneratorFormat() {
    return generatorFormat;
  }

}
