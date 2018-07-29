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
package com.streamsets.pipeline.stage.processor.kudulookup;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.common.MissingValuesBehavior;
import com.streamsets.pipeline.stage.common.MissingValuesBehaviorChooserValues;
import com.streamsets.pipeline.stage.common.MultipleValuesBehavior;
import com.streamsets.pipeline.stage.lib.kudu.KuduFieldMappingConfig;
import com.streamsets.pipeline.stage.processor.kv.CacheConfig;

import java.util.List;

public class KuduLookupConfig {
  public static final String CONF_PREFIX = "conf.";

  // kudu tab
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Kudu Masters",
      description = "Comma-separated list of \"host:port\" pairs of the masters",
      displayPosition = 10,
      group = "KUDU"
  )
  public String kuduMaster;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      elDefs = {RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      defaultValue = "${record:attribute('tableName')}",
      label = "Kudu Table Name",
      description = "Table name to perform lookup",
      displayPosition = 20,
      group = "KUDU"
  )
  public String kuduTableTemplate;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      label = "Key Columns Mapping",
      description = "Specify the columns used as keys for the lookup. For best performance, include primary key columns.",
      displayPosition = 30,
      group = "KUDU"
  )
  @ListBeanModel
  public List<KuduFieldMappingConfig> keyColumnMapping;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      label = "Column to Output Field Mapping",
      description = "Map column names to SDC field names",
      displayPosition = 40,
      group = "KUDU"
  )
  @ListBeanModel
  public List<KuduOutputColumnMapping> outputColumnMapping;

  @ConfigDef(required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Case Sensitive",
      description = "If not set, the table name and all column names are processed in lowercase",
      displayPosition = 50,
      group = "KUDU"
  )
  @ListBeanModel
  public boolean caseSensitive;

  @ConfigDef(required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "SEND_TO_ERROR",
      label = "Missing Lookup Behavior",
      description = "Behavior when lookup did not find a matching record. ",
      displayPosition = 60,
      group = "KUDU"
  )
  @ValueChooserModel(MissingValuesBehaviorChooserValues.class)
  public MissingValuesBehavior missingLookupBehavior = MissingValuesBehavior.SEND_TO_ERROR;

  @ConfigDef(required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Ignore Missing Value in Matching Record",
      description = "Ignore when matching record does not have value in the column " +
          "specified in Column to Output Field Mapping. Otherwise send to error.",
      displayPosition = 70,
      group = "KUDU"
  )
  public boolean ignoreMissing = true;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Multiple Values Behavior",
      description = "How to handle multiple values ",
      defaultValue = "FIRST_ONLY",
      displayPosition = 10,
      group = "ADVANCED"
  )
  @ValueChooserModel(KuduLookupMultipleValuesBehaviorChooserValues.class)
  public MultipleValuesBehavior multipleValuesBehavior = MultipleValuesBehavior.DEFAULT;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "",
      label = "Maximum Number of Worker Threads",
      description = "Set the maximum number of threads to perform lookup processing. If not provided or set to 0, " +
          "the default number (2 * the number of available processors) is used.",
      displayPosition = 20,
      group = "ADVANCED"
  )
  public int numWorkers;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "10000",
      label = "Operation Timeout (milliseconds)",
      description = "Sets the default timeout used for user operations (using sessions and scanners). A value of 0 disables the timeout.",
      displayPosition = 30,
      group = "ADVANCED"
  )
  public int operationTimeout = 10000;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "30000",
      label = "Admin Operation Timeout (milliseconds)",
      description = "Default timeout used for admin operations (openTable, getTableSchema, connectionRetry). " +
          "A value of 0 disables the timeout.",
      displayPosition = 35,
      group = "ADVANCED"
  )
  public int adminOperationTimeout = 30000;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Enable Table Caching",
      description = "Select to enable caching of table information. This improves performance, " +
          "but should only be used when the table schema does not change often",
      displayPosition = 10,
      group = "LOOKUP"
  )
  public boolean enableTableCache;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "-1",
      label = "Maximum Table Entries to Cache",
      description = "Maximum number of table entries to cache. If exceeded, oldest values are evicted to make room. " +
          "Default value is -1 which is unlimited",
      dependsOn = "enableTableCache",
      triggeredByValue = "true",
      displayPosition = 20,
      group = "LOOKUP"
  )
  public int cacheSize = -1;


  @ConfigDefBean(groups = "LOOKUP")
  public CacheConfig cache = new CacheConfig();

}
