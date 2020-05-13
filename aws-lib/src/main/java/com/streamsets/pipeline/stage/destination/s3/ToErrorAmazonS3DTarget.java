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
package com.streamsets.pipeline.stage.destination.s3;

import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.service.ServiceDependency;
import com.streamsets.pipeline.api.service.dataformats.SdcRecordGeneratorService;

@StageDef(
    version = 13,
    label = "Write to Amazon S3",
    description = "Writes error records to Amazon S3",
    upgraderDef = "upgrader/ToErrorAmazonS3DTarget.yaml",
    onlineHelpRefUrl ="index.html?contextID=concept_kgc_l4y_5r",
    services = @ServiceDependency(service = SdcRecordGeneratorService.class)
)
@ErrorStage
@HideStage(HideStage.Type.ERROR_STAGE)
@GenerateResourceBundle
public class ToErrorAmazonS3DTarget extends AmazonS3DTarget {
  @Override
  protected Target createTarget() {
    return new AmazonS3Target(s3TargetConfigBean, true);
  }
}
