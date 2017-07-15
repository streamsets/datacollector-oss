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
package com.streamsets.pipeline.stage.destination.jms;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.configurablestage.DTarget;
import com.streamsets.pipeline.lib.jms.config.InitialContextFactory;
import com.streamsets.pipeline.stage.common.CredentialsConfig;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

@StageDef(
    version = 1,
    label = "JMS Producer",
    description = "Write data to a JMS MQ.",
    icon = "jms.png",
    upgrader = JmsTargetUpgrader.class,
    recordsByRef = true,
    onlineHelpRefUrl = "dunno"
)
@ConfigGroups(JmsTargetGroups.class)
@GenerateResourceBundle
public class JmsDTarget extends DTarget {
  @ConfigDefBean(groups = {"JMS"})
  public CredentialsConfig credentialsConfig;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      displayPosition = 1,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DestinationDataFormatChooserValues.class)
  public DataFormat dataFormat = DataFormat.TEXT;

  @ConfigDefBean(groups = {"JMS"})
  public DataGeneratorFormatConfig dataFormatConfig = new DataGeneratorFormatConfig();

  @ConfigDefBean
  public JmsTargetConfig jmsTargetConfig;

  @Override
  protected Target createTarget() {
    return new JmsTarget(
        credentialsConfig,
        jmsTargetConfig,
        dataFormat,
        dataFormatConfig,
        new JmsMessageProducerFactoryImpl(),
        new InitialContextFactory()
    );
  }
}
