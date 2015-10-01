/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.jms;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ErrorListener;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.configurablestage.DSourceOffsetCommitter;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.CredentialsConfig;
import com.streamsets.pipeline.stage.origin.lib.DataFormatConfig;
import com.streamsets.pipeline.stage.origin.lib.MessageConfig;

@StageDef(
    version = 2,
    label = "JMS Consumer",
    description = "Reads data from a JMS source.",
    icon = "jms.png",
    execution = ExecutionMode.STANDALONE,
    upgrader = JmsSourceUpgrader.class,
    recordsByRef = true
)
@ConfigGroups(value = JmsGroups.class)
@GenerateResourceBundle
public class JmsDSource extends DSourceOffsetCommitter implements ErrorListener {

  @ConfigDefBean(groups = {"JMS"})
  public BasicConfig basicConfig;

  @ConfigDefBean(groups = {"JMS"})
  public CredentialsConfig credentialsConfig;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    label = "Data Format",
    displayPosition = 3000,
    group = "JMS"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDefBean(groups = {"JMS"})
  public DataFormatConfig dataFormatConfig;

  @ConfigDefBean(groups = {"JMS"})
  public MessageConfig messageConfig;

  @ConfigDefBean
  public JmsConfig jmsConfig;

  @Override
  protected Source createSource() {
    return new JmsSource(basicConfig, credentialsConfig, jmsConfig,
      new JmsMessageConsumerFactoryImpl(), new JmsMessageConverterImpl(dataFormat, dataFormatConfig, messageConfig),
      new InitialContextFactory());
  }

  @Override
  public void errorNotification(Throwable throwable) {
    JmsSource source = (JmsSource) getSource();
    source.rollback();
  }
}
