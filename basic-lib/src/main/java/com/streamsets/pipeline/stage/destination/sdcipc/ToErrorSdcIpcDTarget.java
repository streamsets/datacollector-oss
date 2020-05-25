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
package com.streamsets.pipeline.stage.destination.sdcipc;

import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.HideStage;
import com.streamsets.pipeline.api.PipelineLifecycleStage;
import com.streamsets.pipeline.api.StageDef;

@StageDef(
  // We're reusing upgrader for both ToErrorSdcIpcDTarget and SdcIpcDTarget, make sure that you
  // upgrade both versions at the same time when changing.
    version = 3,
    label = "Write to Another Pipeline",
    description = "",
    icon = "",
    onlineHelpRefUrl ="index.html?contextID=concept_kgc_l4y_5r",
    upgrader = SdcIpcTargetUpgrader.class,
    upgraderDef = "upgrader/ToErrorSdcIpcDTarget.yaml"
)
@HideConfigs(
    preconditions = true,
    onErrorRecord = true,
    value = {
        "config.tlsConfigBean.useRemoteKeyStore",
        "config.tlsConfigBean.keyStoreFilePath",
        "config.tlsConfigBean.privateKey",
        "config.tlsConfigBean.certificateChain",
        "config.tlsConfigBean.keyStoreType",
        "config.tlsConfigBean.keyStorePassword",
        "config.tlsConfigBean.keyStoreAlgorithm"
    }
)
@ErrorStage
@PipelineLifecycleStage
@HideStage({HideStage.Type.ERROR_STAGE, HideStage.Type.LIFECYCLE_STAGE})
@GenerateResourceBundle
public class ToErrorSdcIpcDTarget extends SdcIpcDTarget {

}
