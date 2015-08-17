/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.jms;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ErrorListener;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DSourceOffsetCommitter;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.CredentialsConfig;
import com.streamsets.pipeline.stage.origin.lib.DataFormatConfig;

@StageDef(
    version = 1,
    label = "JMS Consumer",
    description = "Reads data from a JMS source.",
    icon = "jms.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true
)
@ConfigGroups(value = JmsGroups.class)
@GenerateResourceBundle
public class JmsDSource extends DSourceOffsetCommitter implements ErrorListener {

  @ConfigDefBean(groups = {"JMS"})
  public BasicConfig basicConfig;

  @ConfigDefBean(groups = {"JMS"})
  public CredentialsConfig credentialsConfig;

  @ConfigDefBean(groups = {"JMS"})
  public DataFormatConfig dataFormatConfig;

  @ConfigDefBean
  public JmsConfig jmsConfig;

  @Override
  protected Source createSource() {
    return new JmsSource(basicConfig, credentialsConfig, dataFormatConfig, jmsConfig,
      new JmsMessageConsumerFactoryImpl(), new JmsMessageConverterImpl(dataFormatConfig),
      new InitialContextFactory());
  }

  @Override
  public void errorNotification(Throwable throwable) {
    JmsSource source = (JmsSource) getSource();
    source.rollback();
  }
}
