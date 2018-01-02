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


import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.configurablestage.DTarget;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.StringEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;

import java.util.List;


@StageDef(
    version = 1,
    label = "Aerospike",
    description = "Writes data to Aerospike",
    icon = "aerospike.png",
    onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class AerospikeDTarget extends DTarget {

  @ConfigDefBean(groups = {"AEROSPIKE"})
  public AerospikeBeanConfig aerospikeBeanConfig;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Namespace",
      description = "Expression to get namespace",
      displayPosition = 30,
      group = "MAPPING",
      elDefs = {RecordEL.class, StringEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String namespaceEL;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Set",
      description = "Expression to get set name",
      displayPosition = 40,
      group = "MAPPING",
      elDefs = {RecordEL.class, StringEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String setEL;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Key",
      description = "Expression to get key",
      displayPosition = 50,
      group = "MAPPING",
      elDefs = {RecordEL.class, StringEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String keyEL;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Bins",
      description = "Bin names and their values",
      displayPosition = 60,
      group = "MAPPING"
  )
  @ListBeanModel
  public List<BinConfig> binConfigsEL;

  @Override
  protected Target createTarget() {
    return new AerospikeTarget(aerospikeBeanConfig, namespaceEL, setEL, keyEL, binConfigsEL);
  }
}
