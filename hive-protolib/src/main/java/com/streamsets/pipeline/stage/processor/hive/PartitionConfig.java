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
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.lib.hive.HiveType;

public final class PartitionConfig {

  public PartitionConfig(
      final String partitionName,
      final HiveType valueType,
      final String valueEL
  ) {
    this.name = partitionName;
    this.valueType = valueType;
    this.valueEL = valueEL;
  }

  /**
   * Parameter-less constructor required.
   */
  public PartitionConfig() {}

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue="/dt",
      label = "Partition Column Name",
      description = "Partition column's name",
      displayPosition = 10
  )
  public String name;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="STRING",
      label = "Partition Value Type",
      description="Partition column's value type",
      displayPosition = 20
  )
  @ValueChooserModel(PartitionColumnTypeChooserValues.class)
  public HiveType valueType = HiveType.STRING;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue="${record:attribute('/dt')}",
      label = "Partition Value Expression",
      description="Expression language to obtain partition value from record",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class},
      displayPosition = 30
  )
  public String valueEL;

}
