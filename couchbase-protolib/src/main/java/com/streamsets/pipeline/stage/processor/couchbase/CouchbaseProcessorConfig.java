/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.couchbase;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.couchbase.BaseCouchbaseConfig;

import java.util.List;

public class CouchbaseProcessorConfig extends BaseCouchbaseConfig {

  /**
   * Lookup Tab
   */
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "KV",
      label = "Lookup Type",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      description = "Lookup type to perform",
      group = "LOOKUP"
  )
  @ValueChooserModel(LookupChooserValues.class)
  public LookupType lookupType = LookupType.KV;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "${record:attribute('doc_id')}",
      label = "Document Key",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      description = "Document key to lookup",
      group = "LOOKUP",
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      dependsOn = "lookupType",
      triggeredByValue = "KV"
  )
  public String documentKeyEL;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Return Properties",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      description = "Return specific sub-document properties rather than the full document",
      group = "LOOKUP",
      dependsOn = "lookupType",
      triggeredByValue = "KV"
  )
  public boolean useSubdoc;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Property Mappings",
      description = "Mappings from sub-document properties to field names",
      displayPosition = 31,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "LOOKUP",
      dependsOn = "useSubdoc",
      triggeredByValue = "true"
  )
  @ListBeanModel
  public List<SubdocMappingConfig> subdocMappingConfigs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "SDC Field",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      description = "Field to write the lookup data in the outgoing record",
      group = "LOOKUP",
      dependsOn = "useSubdoc",
      triggeredByValue = "false"
  )
  public String outputField;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      mode = ConfigDef.Mode.SQL,
      defaultValue = "",
      label = "N1QL Query",
      description = "SELECT <property>, ... FROM <bucket> WHERE <property> <operator> <expression>",
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      dependsOn = "lookupType",
      triggeredByValue = "N1QL",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "LOOKUP"
  )
  public String n1qlQueryEL;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Property Mappings",
      description = "Mappings from N1QL result properties to field names",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "LOOKUP",
      dependsOn = "lookupType",
      triggeredByValue = "N1QL"
  )
  @ListBeanModel
  public List<N1QLMappingConfig> n1qlMappingConfigs;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      label = "Submit as Prepared Statement",
      defaultValue = "true",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.BASIC,
      description = "Submit the query as a prepared statement",
      group = "LOOKUP",
      dependsOn = "lookupType",
      triggeredByValue = "N1QL"
  )
  public boolean n1qlPrepare;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Query Timeout (ms)",
      defaultValue = "75000",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      description = "Server-side timeout for the query",
      group = "LOOKUP",
      dependsOn = "lookupType",
      triggeredByValue = "N1QL"
  )
  public long n1qlTimeout;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Multiple Value Behavior",
      defaultValue = "FIRST",
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      description = "How to handle multiple values",
      group = "LOOKUP",
      dependsOn = "lookupType",
      triggeredByValue = "N1QL"
  )
  @ValueChooserModel(MultipleValueChooserValues.class)
  public MultipleValueType multipleValueOperation = MultipleValueType.FIRST;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Missing Value Behavior",
      defaultValue = "PASS",
      displayPosition = 110,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      description = "How to handle missing values",
      group = "LOOKUP"
  )
  @ValueChooserModel(MissingValueChooserValues.class)
  public MissingValueType missingValueOperation = MissingValueType.PASS;
}
