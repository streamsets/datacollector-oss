/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
  XML("XML", DataParserFormat.XML, DataGeneratorFormat.XML),
  SDC_JSON("SDC Record", DataParserFormat.SDC_RECORD, DataGeneratorFormat.SDC_RECORD),
  LOG("Log", DataParserFormat.LOG, null),
  AVRO("Avro", DataParserFormat.AVRO, DataGeneratorFormat.AVRO),
  BINARY("Binary", DataParserFormat.BINARY, DataGeneratorFormat.BINARY),
  PROTOBUF("Protobuf", DataParserFormat.PROTOBUF, DataGeneratorFormat.PROTOBUF),
  DATAGRAM("Datagram", DataParserFormat.DATAGRAM, null),
  WHOLE_FILE("Whole File", DataParserFormat.WHOLE_FILE, DataGeneratorFormat.WHOLE_FILE),
  SYSLOG("Syslog", DataParserFormat.SYSLOG, null),
  NETFLOW("Netflow", DataParserFormat.NETFLOW, null),
  EXCEL("Excel", DataParserFormat.EXCEL, null),
  FLOWFILE("Nifi FlowFile", DataParserFormat.FLOWFILE, null),
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
