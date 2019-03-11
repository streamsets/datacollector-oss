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
package com.streamsets.pipeline.stage;

import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveType;
import com.streamsets.pipeline.stage.processor.hive.DecimalDefaultsConfig;
import com.streamsets.pipeline.stage.processor.hive.HMPDataFormat;
import com.streamsets.pipeline.stage.processor.hive.HiveMetadataProcessor;
import com.streamsets.pipeline.stage.processor.hive.PartitionConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class HiveMetadataProcessorBuilder {
  private String database;
  private String table;
  private List<PartitionConfig> partitions;
  private boolean external;
  private String tablePathTemplate;
  private String partitionPathTemplate;
  private String timeDriver;
  private DecimalDefaultsConfig decimalDefaultsConfig;
  private TimeZone timeZone;
  private HMPDataFormat dataFormat;
  private String commentEL;
  private Map<String, String> metadataHeaderAttributeConfig;
  private boolean convertTimesToString;

  public HiveMetadataProcessorBuilder() {
    database = "default";
    table = "tbl";
    timeDriver = "${time:now()}";
    partitions = new PartitionConfigBuilder().addPartition("dt", HiveType.STRING, "secret-value").build();
    external = false;
    tablePathTemplate = null;
    partitionPathTemplate = null;
    decimalDefaultsConfig = new DecimalDefaultsConfig();
    decimalDefaultsConfig.scaleExpression = String.valueOf(38);
    decimalDefaultsConfig.precisionExpression = String.valueOf(38);
    timeZone = TimeZone.getTimeZone("UTC");
    dataFormat = HMPDataFormat.AVRO;
    commentEL = "${field:field()}";
    metadataHeaderAttributeConfig = Collections.emptyMap();
    convertTimesToString = false;
  }

  public HiveMetadataProcessorBuilder database(String database) {
    this.database = database;
    return this;
  }

  public HiveMetadataProcessorBuilder table(String table) {
    this.table = table;
    return this;
  }

  public HiveMetadataProcessorBuilder partitions(List<PartitionConfig> partitions) {
    this.partitions = partitions;
    return this;
  }

  public HiveMetadataProcessorBuilder external(boolean external) {
    this.external = external;
    return this;
  }

  public HiveMetadataProcessorBuilder tablePathTemplate(String tablePathTemplate) {
    this.tablePathTemplate = tablePathTemplate;
    return this;
  }

  public HiveMetadataProcessorBuilder partitionPathTemplate(String partitionPathTemplate) {
    this.partitionPathTemplate = partitionPathTemplate;
    return this;
  }

  public HiveMetadataProcessorBuilder timeDriver(String timeDriver) {
    this.timeDriver = timeDriver;
    return this;
  }

  public HiveMetadataProcessorBuilder timeZone(TimeZone timeZone) {
    this.timeZone = timeZone;
    return this;
  }

  public HiveMetadataProcessorBuilder decimalConfig(int precision, int scale) {
    decimalDefaultsConfig = new DecimalDefaultsConfig();
    decimalDefaultsConfig.scaleExpression = String.valueOf(scale);
    decimalDefaultsConfig.precisionExpression = String.valueOf(precision);
    return this;
  }

  public HiveMetadataProcessorBuilder decimalConfig(String precision, String scale) {
    decimalDefaultsConfig = new DecimalDefaultsConfig();
    decimalDefaultsConfig.scaleExpression = scale;
    decimalDefaultsConfig.precisionExpression = precision;
    return this;
  }
  public HiveMetadataProcessorBuilder decimalConfig(DecimalDefaultsConfig decimalDefaultsConfig) {
    this.decimalDefaultsConfig = decimalDefaultsConfig;
    return this;
  }

  public HiveMetadataProcessorBuilder dataFormat(HMPDataFormat dataFormat) {
    this.dataFormat = dataFormat;
    return this;
  }

  public HiveMetadataProcessorBuilder commentEL(String commentEL) {
    this.commentEL = commentEL;
    return this;
  }

  public HiveMetadataProcessorBuilder metadataHeaderAttributeConfig(Map<String, String> metadataHeaderAttributeConfig) {
    this.metadataHeaderAttributeConfig = metadataHeaderAttributeConfig;
    return this;
  }

  public HiveMetadataProcessorBuilder ConvertTimesToString(boolean convertTimesToString) {
    this.convertTimesToString = convertTimesToString;
    return this;
  }

  public HiveMetadataProcessor build() {
    return new HiveMetadataProcessor(
      database,
      table,
      partitions,
      external,
      tablePathTemplate,
      partitionPathTemplate,
      HiveTestUtil.getHiveConfigBean(),
      timeDriver,
      decimalDefaultsConfig,
      timeZone,
      convertTimesToString,
      dataFormat,
      commentEL,
      metadataHeaderAttributeConfig
    );
  }
}
