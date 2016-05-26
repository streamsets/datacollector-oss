/**
 * Copyright 2016 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.lib.hive.typesupport;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.stage.processor.hive.PartitionColumnTypeChooserValues;

public class HiveTypeConfig {
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
      type = ConfigDef.Type.NUMBER,
      defaultValue ="10",
      label = "Partition Value Scale",
      description="Partition column's value Scale",
      displayPosition = 30,
      dependsOn = "valueType",
      triggeredByValue = "DECIMAL"
  )
  public int scale = 10;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "0",
      label = "Partition Value Precision",
      description="Partition column's value Precision",
      displayPosition = 40,
      dependsOn = "valueType",
      triggeredByValue = "DECIMAL"
  )
  public int precision = 0;
}
