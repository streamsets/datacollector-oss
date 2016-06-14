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
package com.streamsets.pipeline.stage.processor.hive;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.lib.el.RecordEL;

public class DecimalDefaultsConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "38",
      label = "Default Scale",
      description = "Default Scale which will be used to create Decimal Columns",
      displayPosition = 130,
      group = "ADVANCED",
      elDefs = {RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String defaultScale;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "38",
      label = "Default Precision",
      description = "Default Precision which will be used to create Decimal Columns",
      displayPosition = 130,
      group = "ADVANCED",
      elDefs = {RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String defaultPrecision;
}
