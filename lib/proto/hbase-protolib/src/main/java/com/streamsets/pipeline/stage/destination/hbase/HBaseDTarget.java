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
package com.streamsets.pipeline.stage.destination.hbase;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.hbase.common.HBaseConnectionConfig;

import java.util.List;

@GenerateResourceBundle
@StageDef(
    version = 3,
    label = "HBase",
    description = "Writes data to HBase",
    icon = "hbase.png",
    privateClassLoader = true,
    upgrader = HBaseTargetUpgrader.class,
    onlineHelpRefUrl ="index.html?contextID=task_pyq_qx5_vr"
)
@ConfigGroups(Groups.class)
public class HBaseDTarget extends DTarget {
  @ConfigDefBean()
  public HBaseConnectionConfig hBaseConnectionConfig = new HBaseConnectionConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Row Key",
      description = "Field path row key",
      displayPosition = 50,
      group = "HBASE")
  public String hbaseRowKey;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TEXT",
      label = "Storage Type",
      description = "The storage type for row key",
      displayPosition = 60,
      group = "HBASE")
  @ValueChooserModel(RowKeyStorageTypeChooserValues.class)
  public StorageType rowKeyStorageType;

  @ConfigDef(required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Fields",
      description = "Column names, their values and storage type",
      displayPosition = 70,
      group = "HBASE")
  @ListBeanModel
  public List<HBaseFieldMappingConfig> hbaseFieldColumnMapping;

  @ConfigDef(required = false,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "true",
    label = "Ignore Missing Field",
    description = "If set, the record will not be treated as error record when a field path is not present in the " +
        "record or if the field value is null",
    displayPosition = 80,
    group = "HBASE")
  public boolean ignoreMissingFieldPath;

  @ConfigDef(required = false,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "false",
    label = "Implicit field mapping",
    description = "If set, field paths will be implicitly mapped to HBase columns; " + "E.g record field cf:a will be inserted"
      + " in the given HBase table with column family 'cf' and qualifier 'a'",
    displayPosition = 90,
    group = "HBASE")
  public boolean implicitFieldMapping;

  @ConfigDef(required = false,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "true",
    label = "Ignore Invalid Column",
    description = "If enabled, field paths that cannot be mapped to column (column family ':' qualifier) will"
      + " be ignored ",
    dependsOn = "implicitFieldMapping",
    triggeredByValue = "true",
    displayPosition = 100,
    group = "HBASE")
  public boolean ignoreInvalidColumn;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Time Basis",
      description = "Time basis to use for cell timestamps. Enter an expression that evaluates to a datetime. To use the " +
          "processing time, enter ${time:now()}. To use field values, use '${record:value(\"<fieldpath>\")}'. If left blank," +
          "system time will be used.",
      displayPosition = 130,
      group = "HBASE",
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String timeDriver;

  @Override
  protected Target createTarget() {
    return new HBaseTarget(
        hBaseConnectionConfig,
        hbaseRowKey,
        rowKeyStorageType,
        hbaseFieldColumnMapping,
        implicitFieldMapping,
        ignoreMissingFieldPath,
        ignoreInvalidColumn,
        timeDriver
    );
  }

}
