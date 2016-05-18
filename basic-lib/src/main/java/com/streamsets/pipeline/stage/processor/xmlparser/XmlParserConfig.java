/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.xmlparser;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.impl.XMLChar;
import com.streamsets.pipeline.config.CharsetChooserValues;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.xml.XmlDataParserFactory;
import com.streamsets.pipeline.stage.common.DataFormatErrors;
import com.streamsets.pipeline.stage.common.DataFormatGroups;

import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.List;

public class XmlParserConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Field to Parse",
      description = "String field that contains a XML document",
      displayPosition = 10,
      group = "XML"
  )
  @FieldSelectorModel(singleValued = true)
  public String fieldPathToParse;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTF-8",
      label = "Charset",
      displayPosition = 20,
      group = "XML"
  )
  @ValueChooserModel(CharsetChooserValues.class)
  public String charset = "UTF-8";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Ignore Control Characters",
      description = "Use only if required as it impacts reading performance",
      displayPosition = 30,
      group = "XML"
  )
  public boolean removeCtrlChars = false;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Delimiter Element",
      defaultValue = "",
      description = "XML element that acts as a record delimiter. No delimiter will treat the whole XML document " +
          "as one record",
      displayPosition = 40,
      group = "XML"
  )
  public String xmlRecordElement = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Target Field",
      description = "Name of the field to set the parsed XML data to",
      displayPosition = 50,
      group = "XML"
  )
  @FieldSelectorModel(singleValued = true)
  public String parsedFieldPath;

  public boolean init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    boolean valid = true;

    try {
      Charset.forName(charset);
    } catch (UnsupportedCharsetException ex) {
      issues.add(context.createConfigIssue("XML", "charset", DataFormatErrors.DATA_FORMAT_05, charset));
      valid = false;
    }
    if (xmlRecordElement != null && !xmlRecordElement.isEmpty() && !XMLChar.isValidName(xmlRecordElement)) {
      issues.add(context.createConfigIssue(
          DataFormatGroups.XML.name(),
          "xmlRecordElement",
          DataFormatErrors.DATA_FORMAT_03,
          xmlRecordElement
      ));
      valid = false;
    }
    return valid;
  }

  public DataParserFactory getParserFactory(Stage.Context context) {
    DataParserFactoryBuilder builder = new DataParserFactoryBuilder(context, DataFormat.XML.getParserFormat());
    try {

      builder.setCharset(Charset.forName(charset));
    } catch (UnsupportedCharsetException ex) {
      throw new RuntimeException("It should not happen: " + ex.toString());
    }

    builder.setRemoveCtrlChars(removeCtrlChars).setMaxDataLen(-1)
        .setConfig(XmlDataParserFactory.RECORD_ELEMENT_KEY, xmlRecordElement);
    return builder.build();
  }

}
