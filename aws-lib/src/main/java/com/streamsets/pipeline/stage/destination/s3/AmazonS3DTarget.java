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
package com.streamsets.pipeline.stage.destination.s3;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;
import com.streamsets.pipeline.api.service.ServiceConfiguration;
import com.streamsets.pipeline.api.service.ServiceDependency;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;
import com.streamsets.pipeline.lib.event.WholeFileProcessedEvent;

@StageDef(
    version = 13,
    label = "Amazon S3",
    description = "Writes to Amazon S3",
    icon = "s3.png",
    privateClassLoader = true,
    upgrader = AmazonS3TargetUpgrader.class,
    upgraderDef = "upgrader/AmazonS3DTarget.yaml",
    producesEvents = true,
    eventDefs = {WholeFileProcessedEvent.class},
    onlineHelpRefUrl ="index.html?contextID=task_pxb_j3r_rt",
    execution = {
        ExecutionMode.STANDALONE,
        ExecutionMode.CLUSTER_BATCH,
        ExecutionMode.CLUSTER_YARN_STREAMING,
        ExecutionMode.CLUSTER_MESOS_STREAMING,
        ExecutionMode.EDGE,
        ExecutionMode.EMR_BATCH
    },
    services = @ServiceDependency(
        service = DataFormatGeneratorService.class,
        configuration = {
            @ServiceConfiguration(
                name = "displayFormats",
                value = "AVRO,BINARY,DELIMITED,JSON,PROTOBUF,SDC_JSON,TEXT,WHOLE_FILE"
            )
        }
    )
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class AmazonS3DTarget extends DTarget {

  @ConfigDefBean()
  public S3TargetConfigBean s3TargetConfigBean;

  @Override
  protected Target createTarget() {
    return new AmazonS3Target(s3TargetConfigBean, false);
  }
}
