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
package com.streamsets.pipeline.stage.destination.mapreduce;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Executor;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.PipelineLifecycleStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DExecutor;
import com.streamsets.pipeline.stage.destination.mapreduce.config.JobConfig;
import com.streamsets.pipeline.stage.destination.mapreduce.config.MapReduceConfig;

@StageDef(
    version = 2,
    upgrader = MapReduceExecutorUpgrader.class,
    label = "MapReduce",
    description = "Starts a MapReduce job",
    icon = "mapreduce-executor.png",
    privateClassLoader = true,
    producesEvents = true,
    onlineHelpRefUrl ="index.html?contextID=task_olh_bmk_fx"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
@PipelineLifecycleStage
public class MapReduceDExecutor extends DExecutor {

  @ConfigDefBean
  public MapReduceConfig mapReduceConfig;

  @ConfigDefBean
  public JobConfig jobConfig;

  @Override
  protected Executor createExecutor() {
    return new MapReduceExecutor(mapReduceConfig, jobConfig);
  }
}
