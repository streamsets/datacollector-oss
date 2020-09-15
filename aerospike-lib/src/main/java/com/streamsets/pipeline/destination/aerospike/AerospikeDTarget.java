/*
 * Copyright 2019 StreamSets Inc.
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
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationActionChooserValues;

import java.util.List;


@StageDef(
    version = 2,
    label = "Aerospike",
    description = "Writes data to Aerospike",
    icon = "aerospike.png",
    upgrader = AerospikeTargetUpgrader.class,
    upgraderDef = "upgrader/AerospikeDTarget.yaml",
    onlineHelpRefUrl = "index.html?contextID=task_j3q_tpr_4cb"
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
      elDefs = {RecordEL.class, TimeNowEL.class},
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
      elDefs = {RecordEL.class, TimeNowEL.class},
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
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String keyEL;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UPSERT",
      label = "Default Operation",
      description = "Default operation to perform if sdc.operation.type is not set in record header.",
      displayPosition = 65,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "MAPPING"
  )
  @ValueChooserModel(AerospikeOperationChooserValues.class)
  public AerospikeOperationType defaultOperation;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue= "USE_DEFAULT",
      label = "Unsupported Operation Handling",
      description = "Action to take when operation type is not supported",
      displayPosition = 67,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "MAPPING"
  )
  @ValueChooserModel(UnsupportedOperationActionChooserValues.class)
  public UnsupportedOperationAction unsupportedAction;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Bins to update",
      description = "Bin names and their values",
      displayPosition = 70,
      group = "MAPPING"
  )
  @ListBeanModel
  public List<BinConfig> binConfigsEL;

  @Override
  protected Target createTarget() {
    return new AerospikeTarget(aerospikeBeanConfig, namespaceEL, setEL, keyEL, binConfigsEL, unsupportedAction, defaultOperation);
  }
}
