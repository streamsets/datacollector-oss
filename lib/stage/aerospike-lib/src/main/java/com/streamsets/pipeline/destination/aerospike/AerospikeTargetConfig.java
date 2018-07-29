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
package com.streamsets.pipeline.destination.aerospike;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.lib.el.RecordEL;

import java.util.List;

public class AerospikeTargetConfig {
  public static final String AEROSPIKE_TARGET_CONFIG_PREFIX = "AerospikeTargetConfig.";


  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "localhost:3000",
      label = "Aerospike nodes",
      description = "Comma-separated list of Aerospike nodes. Use format <HOST>:<PORT>",
      displayPosition = 10,
      group = "AEROSPIKE"
  )
  public String connectionString;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Retry Attempts",
      defaultValue = "1",
      required = true,
      min = 1,
      displayPosition = 20,
      group = "AEROSPIKE"
  )
  public int maxRetries = 1;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "namespace",
      description = "Expression to get namespace",
      displayPosition = 30,
      group = "AEROSPIKE",
      elDefs = {RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String namespaceExpr;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Set",
      description = "Expression to get set",
      displayPosition = 40,
      group = "AEROSPIKE",
      elDefs = {RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String setExpr;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Key",
      description = "Expression to get key",
      displayPosition = 50,
      group = "AEROSPIKE",
      elDefs = {RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String keyExpr;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Bins",
      description = "Bin names and their values",
      displayPosition = 10,
      elDefs = {RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      group = "MAPPING"
  )
  @ListBeanModel
  public List<BinMappingConfig> binMappingConfigs;

}
