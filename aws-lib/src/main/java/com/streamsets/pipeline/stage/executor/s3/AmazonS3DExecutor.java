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
package com.streamsets.pipeline.stage.executor.s3;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Executor;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.PipelineLifecycleStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DExecutor;
import com.streamsets.pipeline.stage.executor.s3.config.AmazonS3ExecutorConfig;

@StageDef(
  version = 3,
  label = "Amazon S3",
  description = "Executes metadata operation on Amazon Simple Storage (S3).",
  icon = "s3.png",
  producesEvents = true,
  upgraderDef = "upgrader/AmazonS3DExecutor.yaml",
  onlineHelpRefUrl ="index.html?contextID=task_nky_cnm_f1b"
)
@ConfigGroups(Groups.class)
@PipelineLifecycleStage
@GenerateResourceBundle
@HideConfigs({
  "config.s3Config.commonPrefix",
  "config.s3Config.delimiter",
})
public class AmazonS3DExecutor extends DExecutor {

  @ConfigDefBean()
  public AmazonS3ExecutorConfig config;

  @Override
  protected Executor createExecutor() {
    return new AmazonS3Executor(config);
  }
}
