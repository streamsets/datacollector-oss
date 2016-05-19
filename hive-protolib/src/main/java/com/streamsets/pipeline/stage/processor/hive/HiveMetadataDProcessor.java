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
package com.streamsets.pipeline.stage.processor.hive;

import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.configurablestage.DProcessor;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.destination.hive.HiveConfigBean;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;

import java.util.List;

@StageDef(
    version=1,
    label="Hive Metadata Processor",
    description = "Extract Hive Metadata, update the schema if record structure changed, and send data to HDFS",
    icon="metadata.png",
    outputStreams = HiveMetadataOutputStreams.class,
    onlineHelpRefUrl = "index.html#Processors/HiveMetadataProcessor.html#" // TODO: add URL here
)

@ConfigGroups(Groups.class)
@HideConfigs(value = {"hiveConfigBean.additionalConfigProperties"})
public class HiveMetadataDProcessor extends DProcessor {

  @ConfigDefBean
  public HiveConfigBean hiveConfigBean;

  @ConfigDef(
      required = true,
      label = "Database Expression",
      type = ConfigDef.Type.STRING,
      defaultValue = "${record:attribute('/database')}",
      description = "Use an expression language to obtain database name from record",
      displayPosition = 20,
      group = "HIVE",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class}
  )
  public String dbNameEL;

  @ConfigDef(
      required = true,
      label = "Table Name",
      type = ConfigDef.Type.STRING,
      defaultValue = "${record:attribute('/table_name')}",
      description = "Use an expression language to obtain table name from record",
      displayPosition = 20,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class},
      group = "HIVE"
  )
  public String tableNameEL;

  @ConfigDef(
      required = true,
      label = "Partition Configuration",
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      description = "Partition information, often used in PARTITION BY clause in CREATE query.",
      displayPosition = 20,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class},
      group = "HIVE"
  )
  @ListBeanModel
  public List<PartitionConfig> partition_list;

  @ConfigDef(
      required = true,
      label = "External Table",
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      description = "Will data be stored in external table? If checked, Hive will not use the default location. " +
          "Otherwise, Hive will use the default location at hive.metastore.warehouse.dir in hive-site.xml",
      displayPosition = 20,
      group = "HIVE"
  )
  public Boolean externalTable;

  @ConfigDef(
      required = false,
      label = "Table Path Template",
      type = ConfigDef.Type.STRING,
      defaultValue = "/user/hive/warehouse/${record:attribute('/database')}.db/${record:attribute('table_name')}",
      description = "Expression for table path",
      displayPosition = 20,
      group = "HIVE",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class},
      dependsOn = "externalTable",
      triggeredByValue = "false"
  )
  public String warehouseDirectory;

  /* Only when internal checkbox is set to NO */
  @ConfigDef(
      required = false,
      label = "Table Path Template",
      type = ConfigDef.Type.STRING,
      defaultValue = "/user/hive/warehouse/${record:attribute('/database')}.db/${record:attribute('table_name')}",
      description = "Expression for table path",
      displayPosition = 20,
      group = "HIVE",
      dependsOn = "externalTable",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class},
      triggeredByValue = "true"
  )
  public String tablePathTemplate;

  @ConfigDef(
      required = false,
      label = "Partition Path Template",
      type = ConfigDef.Type.STRING,
      defaultValue = "dt=${record:attribute('/date')}",
      description = "Expression for partition path",
      displayPosition = 20,
      group = "HIVE",
      dependsOn = "externalTable",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class},
      triggeredByValue = "true"
  )
  public String partitionPathTemplate;

  @Override
  protected Processor createProcessor() {
    return new HiveMetadataProcessor(
        dbNameEL, tableNameEL,partition_list,
        externalTable,warehouseDirectory,
        tablePathTemplate, partitionPathTemplate, hiveConfigBean);
  }

}
