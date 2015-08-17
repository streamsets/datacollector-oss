/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.jms;

import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.CredentialsConfig;

import javax.jms.ConnectionFactory;
import javax.naming.InitialContext;

public class JmsMessageConsumerFactoryImpl implements JmsMessageConsumerFactory {

  @Override
  public JmsMessageConsumer create(InitialContext initialContext, ConnectionFactory connectionFactory,
                         BasicConfig basicConfig, CredentialsConfig credentialsConfig,
                         JmsConfig jmsConfig, JmsMessageConverter jmsMessageConverter) {
    return new JmsMessageConsumerImpl(initialContext, connectionFactory, basicConfig, credentialsConfig,
      jmsConfig, jmsMessageConverter);
  }
}
