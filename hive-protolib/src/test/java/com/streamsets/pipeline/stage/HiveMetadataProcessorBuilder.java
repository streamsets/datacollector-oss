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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage;

import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveType;
import com.streamsets.pipeline.stage.processor.hive.HiveMetadataProcessor;
import com.streamsets.pipeline.stage.processor.hive.PartitionConfig;

import java.util.List;

public class HiveMetadataProcessorBuilder {
  private String database;
  private String table;
  private List<PartitionConfig> partitions;
  private boolean external;
  private String tablePathTemplate;
  private String partitionPathTemplate;

  public HiveMetadataProcessorBuilder() {
    database = "default";
    table = "tbl";
    partitions = new PartitionConfigBuilder().addPartition("dt", HiveType.STRING, "secret-value").build();
    external = false;
    tablePathTemplate = null;
    partitionPathTemplate = null;
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

  public HiveMetadataProcessor build() {
    return new HiveMetadataProcessor(
      database,
      table,
      partitions,
      external,
      tablePathTemplate,
      partitionPathTemplate,
      BaseHiveIT.getHiveConfigBean()
    );
  }
}
