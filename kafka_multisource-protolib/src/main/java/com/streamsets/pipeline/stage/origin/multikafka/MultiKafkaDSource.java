/**
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
package com.streamsets.pipeline.stage.origin.multikafka;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DPushSource;

@StageDef(
    version = 9,
    label = "Kafka Multitopic Consumer",
    description = "Reads data from multiple topics of a Kafka Broker",
    execution = ExecutionMode.STANDALONE,
    icon = "kafka.png",
    recordsByRef = true,
    upgrader = MultiKafkaSourceUpgrader.class,
    upgraderDef = "upgrader/MultiKafkaDSource.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_ost_3n4_x1b"
)
@RawSource(rawSourcePreviewer = MultiKafkaRawSourcePreviewer.class,  mimeType = "*/*")
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class MultiKafkaDSource extends DPushSource {

  @ConfigDefBean
  public MultiKafkaBeanConfig conf;

  @Override
  protected PushSource createPushSource() {
    return new MultiKafkaSource(conf);
  }
}
