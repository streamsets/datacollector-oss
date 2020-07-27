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
package com.streamsets.pipeline.stage.processor.xmlparser;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.CharsetChooserValues;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.xml.XmlDataParserFactory;
import com.streamsets.pipeline.lib.xml.Constants;
import com.streamsets.pipeline.lib.xml.xpath.XPathValidatorUtil;
import com.streamsets.pipeline.stage.common.DataFormatErrors;
import com.streamsets.pipeline.stage.common.MultipleValuesBehavior;
import com.streamsets.pipeline.stage.common.MultipleValuesBehaviorChooserValues;
import org.apache.commons.lang.StringUtils;

import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class XmlParserConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Field to Parse",
      description = "String field that contains a XML document",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
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
      displayMode = ConfigDef.DisplayMode.ADVANCED,
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
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "XML"
  )
  public boolean removeCtrlChars = false;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Delimiter Element",
      defaultValue = "",
      description = Constants.XML_RECORD_ELEMENT_DESCRIPTION,
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "XML"
  )
  public String xmlRecordElement = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Include Field XPaths",
      defaultValue = ""+XmlDataParserFactory.INCLUDE_FIELD_XPATH_ATTRIBUTES_DEFAULT,
      description = Constants.INCLUDE_FIELD_XPATH_ATTRIBUTES_DESCRIPTION,
      displayPosition = 43,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "XML"
  )
  public boolean includeFieldXpathAttributes = XmlDataParserFactory.INCLUDE_FIELD_XPATH_ATTRIBUTES_DEFAULT;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Namespaces",
      description = Constants.XPATH_NAMESPACE_CONTEXT_DESCRIPTION,
      defaultValue = "{}",
      displayPosition = 45,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "XML"
  )
  public Map<String, String> xPathNamespaceContext = new HashMap<>();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Output Field Attributes",
      description = Constants.OUTPUT_FIELD_ATTRIBUTES_DESCRIPTION,
      defaultValue = ""+XmlDataParserFactory.USE_FIELD_ATTRIBUTES_DEFAULT,
      displayPosition = 48,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "XML"
  )
  public boolean outputFieldAttributes = XmlDataParserFactory.USE_FIELD_ATTRIBUTES_DEFAULT;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Target Field",
      description = "Name of the field to set the parsed XML data to",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "XML"
  )
  @FieldSelectorModel(singleValued = true)
  public String parsedFieldPath;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Multiple Values Behavior",
      description = "How to handle multiple values produced by the parser",
      defaultValue = "FIRST_ONLY",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "XML"
  )
  @ValueChooserModel(MultipleValuesBehaviorChooserValues.class)
  public MultipleValuesBehavior multipleValuesBehavior = MultipleValuesBehavior.DEFAULT;

  public boolean init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    boolean valid = true;

    try {
      Charset.forName(charset);
    } catch (UnsupportedCharsetException ex) {
      issues.add(context.createConfigIssue("XML", "charset", DataFormatErrors.DATA_FORMAT_05, charset));
      valid = false;
    }
    if (StringUtils.isNotBlank(xmlRecordElement)) {
      String invalidXPathError = XPathValidatorUtil.getXPathValidationError(xmlRecordElement);
      if (StringUtils.isNotBlank(invalidXPathError)) {
        issues.add(context.createConfigIssue(Groups.XML.name(),
            "xmlRecordElement",
            DataFormatErrors.DATA_FORMAT_03,
            xmlRecordElement,
            invalidXPathError
        ));
        valid = false;
      } else {
        final Set<String> nsPrefixes = XPathValidatorUtil.getNamespacePrefixes(xmlRecordElement);
        nsPrefixes.removeAll(xPathNamespaceContext.keySet());
        if (!nsPrefixes.isEmpty()) {
          issues.add(context.createConfigIssue(Groups.XML.name(),
              "xPathNamespaceContext",
              DataFormatErrors.DATA_FORMAT_304,
              StringUtils.join(nsPrefixes, ", ")
          ));
          valid = false;
        }
      }
    }
    return valid;
  }

  public DataParserFactory getParserFactory(Stage.Context context) {
    DataParserFactoryBuilder builder = new DataParserFactoryBuilder(context, DataFormat.XML.getParserFormat());
    try {

      builder.setCharset(Charset.forName(charset));
    } catch (UnsupportedCharsetException ex) {
      throw new RuntimeException("It should not happen: " + ex.toString(), ex);
    }

    builder.setRemoveCtrlChars(removeCtrlChars).setMaxDataLen(-1)
        .setConfig(XmlDataParserFactory.RECORD_ELEMENT_KEY, xmlRecordElement)
        .setConfig(XmlDataParserFactory.INCLUDE_FIELD_XPATH_ATTRIBUTES_KEY, includeFieldXpathAttributes)
        .setConfig(XmlDataParserFactory.RECORD_ELEMENT_XPATH_NAMESPACES_KEY, xPathNamespaceContext)
        .setConfig(XmlDataParserFactory.USE_FIELD_ATTRIBUTES, outputFieldAttributes);
    return builder.build();
  }

}
