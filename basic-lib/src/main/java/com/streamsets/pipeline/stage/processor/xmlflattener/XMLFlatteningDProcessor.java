/**
 * Copyright 2015 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.xmlflattener;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DProcessor;


@StageDef(
    version = 1,
    label = "XML Flattener",
    description = "Flatten XML data into fields of a record",
    icon = "xmlparser.png",
    onlineHelpRefUrl = "index.html#Processors/XMLFlattener.html#task_pmb_l55_sv"
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class XMLFlatteningDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = "XML Field",
      description = "The field containing XML to flatten.",
      displayPosition = 10,
      group = "XML"
  )
  @FieldSelectorModel(singleValued = true)
  public String fromField;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue="",
      label = "Record Delimiter",
      description = "The tag used to delimit records. If this is not specified, only a single record is generated.",
      displayPosition = 30,
      group = "XML"
  )
  public String recordDelimiter;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue=".",
      label = "Field Delimiter",
      description = "The string used to separate entity names in the flattened field names.",
      displayPosition = 30,
      group = "XML"
  )
  public String fieldDelimiter;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue="#",
      label = "Attribute Delimiter",
      description = "The string used to separate attributes in the flattened field names.",
      displayPosition = 50,
      group = "XML"
  )
  public String attrDelimiter;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Ignore Attributes",
      description = "Whether attributes of tags should be ignored.",
      displayPosition = 60,
      group = "XML"
  )
  public boolean ignoreAttributes;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Ignore Namespace URI",
      description = "Whether namespace URIs should be ignored.",
      displayPosition = 70,
      group = "XML"
  )
  public boolean ignoreNamespace;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Ignore Empty Values",
      description = "Whether entities with blank values should be ignored.",
      displayPosition = 80,
      group = "XML"
  )
  public boolean ignoreEmptyValues;

  @Override
  protected Processor createProcessor() {
    return new XMLFlatteningProcessor(fromField, recordDelimiter, fieldDelimiter, attrDelimiter,
        ignoreAttributes, ignoreNamespace, ignoreEmptyValues);
  }
}
