/**
 * Copyright 2016 StreamSets Inc.
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

package com.streamsets.pipeline.stage.destination.kudu;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.configurablestage.DTarget;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;

import java.util.List;

@GenerateResourceBundle
@StageDef(
    version = 2,
    label = "Kudu",
    description = "Writes data to Kudu",
    icon = "kudu.png",
    privateClassLoader = true,
    onlineHelpRefUrl = "index.html#Destinations/Kudu.html#task_c4x_tmh_4v",
    upgrader = KuduTargetUpgrader.class
)
@ConfigGroups(Groups.class)
public class KuduDTarget extends DTarget {

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
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      defaultValue = "${record:attribute('tableName')}",
      label = "Table Name",
      description = "Kudu table to write to. If table doesn't exist, records will be treated as error records.",
      displayPosition = 20,
      group = "KUDU"
  )
  public String tableNameTemplate;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Field to Column Mapping",
      description = "Optionally specify additional field mappings when input field name and column name don't match.",
      displayPosition = 30,
      group = "KUDU"
  )
  @ListBeanModel
  public List<KuduFieldMappingConfig> fieldMappingConfigs;


  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "CLIENT_PROPAGATED",
    label = "External Consistency",
    description = "The external consistency mode",
    displayPosition = 10,
    group = "ADVANCED"
  )
  @ValueChooserModel(ConsistencyModeChooserValues.class)
  public ConsistencyMode consistencyMode;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "10000",
    label = "Operation Timeout Milliseconds",
    description = "Sets the default timeout used for user operations (using sessions and scanners)",
    displayPosition = 20,
    group = "ADVANCED"
  )
  public int operationTimeout;

  @Override
  protected Target createTarget() {
    return new KuduTarget(kuduMaster, tableNameTemplate, consistencyMode, fieldMappingConfigs, operationTimeout);

  }

}
