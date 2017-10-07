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
import com.streamsets.pipeline.stage.common.MultipleValuesBehavior;
import com.streamsets.pipeline.stage.lib.kudu.KuduFieldMappingConfig;
import com.streamsets.pipeline.stage.processor.kv.CacheConfig;

import java.util.List;

public class KuduLookupConfig {
  public static final String CONF_PREFIX = "kuduLookupConfig.";

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
      description = "Specify the columns used as keys for the lookup. The mapping must include a primary key column",
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
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Ignore Missing Value",
      description = "If set, process records even if column values are missing. If not set, send records to error",
      displayPosition = 60,
      group = "KUDU"
  )
  @ListBeanModel
  public boolean ignoreMissing;

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
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "10000",
      label = "Operation Timeout Milliseconds",
      description = "Sets the default timeout used for user operations (using sessions and scanners)",
      displayPosition = 30,
      group = "ADVANCED"
  )
  public int operationTimeout;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Cache Kudu Table",
      description = "Cache table objects. Enable only if table schema won't change often. This will improve performance.",
      displayPosition = 10,
      group = "LOOKUP"
  )
  public boolean enableTableCache;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "-1",
      label = "Maximum Entries to Cache Table Objects",
      description = "Maximum number of object to cache. If exceeded, oldest values are evicted to make room. Default value is -1 which is unlimited",
      dependsOn = "enableTableCache",
      triggeredByValue = "true",
      displayPosition = 20,
      group = "LOOKUP"
  )
  public int cacheSize = -1;


  @ConfigDefBean(groups = "LOOKUP")
  public CacheConfig cache = new CacheConfig();

}
