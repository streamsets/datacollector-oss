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
package com.streamsets.pipeline.stage.processor.expression;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageBehaviorFlags;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;

import java.util.List;

@StageDef(
    version=2,
    label="Expression Evaluator",
    description="Performs calculations on a field-by-field basis",
    icon="expression.png",
    upgrader = ExpressionProcessorUpgrader.class,
    upgraderDef = "upgrader/ExpressionDProcessor.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_x2h_tv4_yq",
    execution = {
        ExecutionMode.STANDALONE,
        ExecutionMode.CLUSTER_BATCH,
        ExecutionMode.CLUSTER_YARN_STREAMING,
        ExecutionMode.CLUSTER_MESOS_STREAMING,
        ExecutionMode.EDGE,
        ExecutionMode.EMR_BATCH

    }
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class ExpressionDProcessor extends DProcessor {

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Field Expressions",
      displayPosition = 10,
      group = "EXPRESSIONS"
  )
  @ListBeanModel
  public List<ExpressionProcessorConfig> expressionProcessorConfigs;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.MODEL,
    label = "Header Attribute Expressions",
    displayPosition = 20,
    group = "EXPRESSIONS"
  )
  @ListBeanModel
  public List<HeaderAttributeConfig> headerAttributeConfigs;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Field Attribute Expressions",
      displayPosition = 30,
      group = "EXPRESSIONS"
  )
  @ListBeanModel
  public List<FieldAttributeConfig> fieldAttributeConfigs;

  @Override
  protected Processor createProcessor() {
    return new ExpressionProcessor(expressionProcessorConfigs, headerAttributeConfigs, fieldAttributeConfigs);
  }

}
