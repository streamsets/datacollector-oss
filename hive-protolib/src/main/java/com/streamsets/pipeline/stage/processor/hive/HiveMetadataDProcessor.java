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
package com.streamsets.pipeline.stage.processor.hive;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.TimeZoneChooserValues;
import com.streamsets.pipeline.configurablestage.DProcessor;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.stage.lib.hive.FieldPathEL;
import com.streamsets.pipeline.stage.lib.hive.HiveConfigBean;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

@StageDef(
    version = 2,
    label="Hive Metadata",
    description = "Generates Hive metadata and write information for HDFS",
    icon="metadata.png",
    outputStreams = HiveMetadataOutputStreams.class,
    privateClassLoader = true,
    onlineHelpRefUrl = "index.html#Processors/HiveMetadata.html#task_hpg_pft_zv",
    upgrader = HiveMetadataProcessorUpgrader.class
)

@ConfigGroups(Groups.class)
public class HiveMetadataDProcessor extends DProcessor {

  @ConfigDefBean
  public HiveConfigBean hiveConfigBean;

  @ConfigDef(
      required = false,
      label = "Database Expression",
      type = ConfigDef.Type.STRING,
      defaultValue = "${record:attribute('database')}",
      description = "Use an expression language to obtain database name from record. If not set, \"default\" will be applied",
      displayPosition = 10,
      group = "TABLE",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class}
  )
  public String dbNameEL;

  @ConfigDef(
      required = true,
      label = "Table Name",
      type = ConfigDef.Type.STRING,
      defaultValue = "${record:attribute('table_name')}",
      description = "Use an expression to obtain the table name from the record. Note that Hive changes the name to lowercase when creating a table.",
      displayPosition = 20,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class},
      group = "TABLE"
  )
  public String tableNameEL;

  @ConfigDef(
      required = true,
      label = "Partition Configuration",
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      description = "Partition information, often used in PARTITION BY clause in CREATE query.",
      displayPosition = 30,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class},
      group = "TABLE"
  )
  @ListBeanModel
  public List<PartitionConfig> partitionList;

  @ConfigDef(
      required = true,
      label = "External Table",
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      description = "Will data be stored in external table? If checked, Hive will not use the default location. " +
          "Otherwise, Hive will use the default location at hive.metastore.warehouse.dir in hive-site.xml",
      displayPosition = 40,
      group = "TABLE"
  )
  public boolean externalTable;

  /* Only when internal checkbox is set to NO */
  @ConfigDef(
      required = false,
      label = "Table Path Template",
      type = ConfigDef.Type.STRING,
      defaultValue = "/user/hive/warehouse/${record:attribute('database')}.db/${record:attribute('table_name')}",
      description = "Expression for table path",
      displayPosition = 50,
      group = "TABLE",
      dependsOn = "externalTable",
      triggeredByValue = "true",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class}
  )
  public String tablePathTemplate;

  @ConfigDef(
      required = false,
      label = "Partition Path Template",
      type = ConfigDef.Type.STRING,
      defaultValue = "dt=${record:attribute('dt')}",
      description = "Expression for partition path",
      displayPosition = 60,
      group = "TABLE",
      dependsOn = "externalTable",
      triggeredByValue = "true",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class}
  )
  public String partitionPathTemplate;

  @ConfigDef(
      required = false,
      label = "Column comment",
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      description = "Expression that will evaluate to column comment.",
      displayPosition = 70,
      group = "TABLE",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class, FieldPathEL.class}
  )
  public String commentExpression;

  @ConfigDefBean
  public DecimalDefaultsConfig decimalDefaultsConfig;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "${time:now()}",
      label = "Time Basis",
      description = "Time basis to use for a record. Enter an expression that evaluates to a datetime. To use the " +
          "processing time, enter ${time:now()}. To use field values, use '${record:value(\"<filepath>\")}'.",
      displayPosition = 100,
      group = "ADVANCED",
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String timeDriver;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTC",
      label = "Data Time Zone",
      description = "Time zone to use for a record.",
      displayPosition = 110,
      group = "ADVANCED"
  )
  @ValueChooserModel(TimeZoneChooserValues.class)
  public String timeZoneID;

  @ConfigDef(
      required = false,
      defaultValue = "{}",
      type = ConfigDef.Type.MAP,
      label = "Header Attribute Expressions",
      description = "Header attributes to insert into the metadata record output",
      displayPosition = 120,
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      group = "ADVANCED"
  )
  @ListBeanModel
  public Map<String, String> metadataHeaderAttributeConfigs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      displayPosition = 10,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(HMPDataFormatChooserValues.class)
  public HMPDataFormat dataFormat = HMPDataFormat.AVRO;

  @Override
  protected Processor createProcessor() {
    return new HiveMetadataProcessor(
      dbNameEL,
      tableNameEL,
      partitionList,
      externalTable,
      tablePathTemplate,
      partitionPathTemplate,
      hiveConfigBean,
      timeDriver,
      decimalDefaultsConfig,
      TimeZone.getTimeZone(ZoneId.of(timeZoneID)),
      dataFormat,
      commentExpression,
      metadataHeaderAttributeConfigs
    );
  }

}
