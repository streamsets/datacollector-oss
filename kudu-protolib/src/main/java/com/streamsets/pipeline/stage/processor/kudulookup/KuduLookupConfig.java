/**
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.kudulookup;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
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
      label = "Kudu Table",
      description = "Table name, case sensitive",
      displayPosition = 20,
      group = "KUDU"
  )
  public String kuduTable;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      label = "Key Columns Mapping",
      description = "Specify columns which will be used as key for scan. This must include primary key columns.",
      displayPosition = 30,
      group = "KUDU"
  )
  @ListBeanModel
  public List<KuduFieldMappingConfig> keyColumnMapping;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      label = "Column to Output Field Mapping",
      description = "Specify field name to write the result",
      displayPosition = 40,
      group = "KUDU"
  )
  @ListBeanModel
  public List<KuduOutputColumnMapping> outputColumnMapping;

  @ConfigDef(required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Case Sensitive",
      description = "If not set, table name and all column names are processed in lowercase",
      displayPosition = 50,
      group = "KUDU"
  )
  @ListBeanModel
  public boolean caseSensitive;

  @ConfigDef(required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Ignore Missing Value",
      description = "If set, process records even if values specified above Output Column to Field Mapping are missing. " +
          "If not set, send records to error.",
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

  @ConfigDefBean(groups = "LOOKUP")
  public CacheConfig cache = new CacheConfig();
}
