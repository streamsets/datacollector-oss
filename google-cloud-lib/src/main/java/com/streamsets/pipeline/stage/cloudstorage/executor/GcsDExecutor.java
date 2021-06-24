/*
 * Copyright 2021 StreamSets Inc.
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
package com.streamsets.pipeline.stage.cloudstorage.executor;

import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.configurablestage.DExecutor;
import com.streamsets.pipeline.stage.cloudstorage.executor.config.GcsExecutorConfig;

@StageDef(
    version = 1,
    label = "Google Cloud Storage Executor",
    description = "Executes metadata operation on Google Cloud Storage.",
    icon = "cloud-storage-logo.png",
    producesEvents = true,
    upgraderDef = "upgrader/GoogleCloudStorageDExecutor.yaml",
    onlineHelpRefUrl = ""
)
@ConfigGroups(Groups.class)
@PipelineLifecycleStage
@GenerateResourceBundle
public class GcsDExecutor extends DExecutor {

  @ConfigDefBean()
  public GcsExecutorConfig config;

  @Override
  protected Executor createExecutor() {
    return new GcsExecutor(config);
  }
}
