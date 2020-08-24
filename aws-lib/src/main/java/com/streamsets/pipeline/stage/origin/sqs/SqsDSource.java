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
package com.streamsets.pipeline.stage.origin.sqs;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DPushSource;
import com.streamsets.pipeline.api.service.ServiceConfiguration;
import com.streamsets.pipeline.api.service.ServiceDependency;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;

@StageDef(
    version = 6,
    label = "Amazon SQS Consumer",
    description = "Reads messages from Amazon SQS",
    icon = "sqs.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    resetOffset = true,
    onlineHelpRefUrl ="index.html?contextID=task_jxn_nnm_5bb",
    upgrader = SqsUpgrader.class,
    upgraderDef = "upgrader/SqsDSource.yaml",
    services = @ServiceDependency(
      service = DataFormatParserService.class,
      configuration = {
        @ServiceConfiguration(name = "displayFormats", value = "AVRO,BINARY,DELIMITED,JSON,LOG,PROTOBUF,SDC_JSON,TEXT,XML")
      }
    )
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class SqsDSource extends DPushSource {

  @ConfigDefBean(groups = {"SQS", "ADVANCED"})
  public SqsConsumerConfigBean sqsConfig;

  @Override
  protected PushSource createPushSource() {
    return new SqsConsumer(sqsConfig);
  }
}
