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
package com.streamsets.pipeline.stage.destination.jms;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;
import com.streamsets.pipeline.api.service.ServiceConfiguration;
import com.streamsets.pipeline.api.service.ServiceDependency;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;
import com.streamsets.pipeline.lib.jms.config.InitialContextFactory;

@StageDef(
    version = 3,
    label = "JMS Producer",
    description = "Write data to a JMS MQ.",
    icon = "jms.png",
    upgrader = JmsTargetUpgrader.class,
    upgraderDef = "upgrader/JmsDTarget.yaml",
    recordsByRef = true,
    onlineHelpRefUrl ="index.html?contextID=task_udk_yw5_n1b",
    services = @ServiceDependency(
      service = DataFormatGeneratorService.class,
      configuration = {
        @ServiceConfiguration(name = "displayFormats", value = "AVRO,BINARY,DELIMITED,JSON,PROTOBUF,SDC_JSON,TEXT,XML")
      }
    )
)
@ConfigGroups(JmsTargetGroups.class)
@GenerateResourceBundle
public class JmsDTarget extends DTarget {

  @ConfigDefBean
  public JmsTargetConfig jmsTargetConfig;

  @Override
  protected Target createTarget() {
    return new JmsTarget(
        jmsTargetConfig,
        new JmsMessageProducerFactoryImpl(),
        new InitialContextFactory()
    );
  }
}
