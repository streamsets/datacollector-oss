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
package com.streamsets.datacollector.pipeline.executor.spark;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Executor;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.PipelineLifecycleStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DExecutor;

@StageDef(
    version = 3,
    label = "Spark",
    description = "Run Spark Applications",
    icon = "spark-logo-hd.png",
    upgrader = SparkExecutorUpgrader.class,
    upgraderDef = "upgrader/SparkDExecutor.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_cdw_wxb_1z",
    producesEvents = true
)
@ConfigGroups(Groups.class)
@HideConfigs("conf.yarnConfigBean.waitForCompletion")
@GenerateResourceBundle
@PipelineLifecycleStage
public class SparkDExecutor extends DExecutor {

  @ConfigDefBean
  public SparkExecutorConfigBean conf = new SparkExecutorConfigBean();

  @Override
  protected Executor createExecutor() {
    return new SparkExecutor(conf);
  }
}
