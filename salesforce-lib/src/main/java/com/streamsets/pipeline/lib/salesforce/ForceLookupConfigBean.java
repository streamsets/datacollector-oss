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
package com.streamsets.pipeline.lib.salesforce;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.stage.common.MissingValuesBehavior;
import com.streamsets.pipeline.stage.common.MissingValuesBehaviorChooserValues;
import com.streamsets.pipeline.stage.common.MultipleValuesBehavior;
import com.streamsets.pipeline.stage.common.MultipleValuesBehaviorChooserValues;
import com.streamsets.pipeline.stage.processor.kv.CacheConfig;

import java.util.List;

public class ForceLookupConfigBean extends ForceInputConfigBean {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "QUERY",
      label = "Lookup Mode",
      description = "Lookup records by a SOQL Query or by the Salesforce record ID.",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "QUERY"
  )
  @ValueChooserModel(LookupModeChooserValues.class)
  public LookupMode lookupMode;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      mode = ConfigDef.Mode.SQL,
      defaultValue = "",
      label = "SOQL Query",
      description =
          "SELECT <field>, ... FROM <object name> WHERE <field> <operator> <expression>",
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      dependsOn = "lookupMode",
      triggeredByValue = "QUERY",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "QUERY"
  )
  public String soqlQuery;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Use Bulk API",
      description = "If enabled, records will be read and written via the Salesforce Bulk API, " +
          "otherwise, the Salesforce SOAP API will be used.",
      displayPosition = 72,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "lookupMode",
      triggeredByValue = "QUERY",
      group = "QUERY"
  )
  public boolean useBulkAPI;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Id Field",
      description = "The field in the record containing the Salesforce record ID for lookup",
      defaultValue = "",
      dependsOn = "lookupMode",
      triggeredByValue = "RETRIEVE",
      displayPosition = 75,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "QUERY"
  )
  @FieldSelectorModel(singleValued = true)
  public String idField = "";

  // This is a comma-separated string, since (1) a common use case is to
  // copy the field names from an existing SOQL query and (2) the Salesforce
  // retrieve() call takes a comma-separated list of field names
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Salesforce Fields",
      description = "Comma-separated list of Salesforce fields to retrieve for each record",
      defaultValue = "",
      dependsOn = "lookupMode",
      triggeredByValue = "RETRIEVE",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "QUERY"
  )
  public String retrieveFields = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Object Type",
      description = "The Salesforce object type to retrieve",
      defaultValue = "",
      dependsOn = "lookupMode",
      triggeredByValue = "RETRIEVE",
      displayPosition = 85,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "QUERY"
  )
  public String sObjectType = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Field Mappings",
      defaultValue = "",
      description = "Mappings from Salesforce field names to SDC field names",
      displayPosition = 90,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "QUERY"
  )
  @ListBeanModel
  public List<ForceSDCFieldMapping> fieldMappings;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Multiple Values Behavior",
      description = "How to handle multiple values",
      defaultValue = "FIRST_ONLY",
      displayPosition = 95,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "QUERY"
  )
  @ValueChooserModel(MultipleValuesBehaviorChooserValues.class)
  public MultipleValuesBehavior multipleValuesBehavior = MultipleValuesBehavior.DEFAULT;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Missing Values Behavior",
      description = "How to handle missing values when no default value is defined.",
      defaultValue = "PASS_RECORD_ON",
      displayPosition = 97,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "QUERY"
  )
  @ValueChooserModel(MissingValuesBehaviorChooserValues.class)
  public MissingValuesBehavior missingValuesBehavior = MissingValuesBehavior.DEFAULT;

  @ConfigDefBean(groups = "QUERY")
  public CacheConfig cacheConfig = new CacheConfig();

  public ForceLookupConfigBean() {
    queryExistingData = true;
  }
}
