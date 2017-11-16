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
import com.streamsets.pipeline.lib.el.StringEL;
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
      group = "LOOKUP"
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
      elDefs = {StringEL.class, RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      dependsOn = "lookupMode",
      triggeredByValue = "QUERY",
      displayPosition = 60,
      group = "LOOKUP"
  )
  public String soqlQuery;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Include Deleted Records",
      description = "When enabled, the processor will additionally retrieve deleted records from the Recycle Bin",
      defaultValue = "false",
      dependsOn = "lookupMode",
      triggeredByValue = "QUERY",
      displayPosition = 70,
      group = "LOOKUP"
  )
  public boolean queryAll = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Id Field",
      description = "The field in the record containing the Salesforce record ID for lookup",
      defaultValue = "",
      dependsOn = "lookupMode",
      triggeredByValue = "RETRIEVE",
      displayPosition = 80,
      group = "LOOKUP"
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
      displayPosition = 90,
      group = "LOOKUP"
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
      displayPosition = 95,
      group = "LOOKUP"
  )
  public String sObjectType = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Field Mappings",
      defaultValue = "",
      description = "Mappings from Salesforce field names to SDC field names",
      displayPosition = 100,
      group = "LOOKUP"
  )
  @ListBeanModel
  public List<ForceSDCFieldMapping> fieldMappings;

  @ConfigDefBean(groups = "LOOKUP")
  public CacheConfig cacheConfig = new CacheConfig();
}
