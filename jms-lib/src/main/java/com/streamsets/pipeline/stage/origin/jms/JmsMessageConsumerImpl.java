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
package com.streamsets.pipeline.stage.origin.jms;


import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.jms.config.JmsErrors;
import com.streamsets.pipeline.lib.jms.config.JmsGroups;
import com.streamsets.pipeline.stage.common.CredentialsConfig;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.ArrayList;
import java.util.List;

public class JmsMessageConsumerImpl implements JmsMessageConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(JmsMessageConsumerImpl.class);
  private static final int POLL_INTERVAL = 100; // ms
  private static final boolean IS_TRACE_ENABLED = LOG.isTraceEnabled();
  private final InitialContext initialContext;
  private final ConnectionFactory connectionFactory;
  private final BasicConfig basicConfig;
  private final CredentialsConfig credentialsConfig;
  private final JmsSourceConfig jmsConfig;
  private final JmsMessageConverter jmsMessageConverter;
  private Connection connection;
  private Session session;
  private Destination destination;
  private MessageConsumer messageConsumer;

  public JmsMessageConsumerImpl(InitialContext initialContext, ConnectionFactory connectionFactory,
                                BasicConfig basicConfig, CredentialsConfig credentialsConfig,
                                JmsSourceConfig jmsConfig, JmsMessageConverter jmsMessageConverter) {
    this.initialContext = initialContext;
    this.connectionFactory = connectionFactory;
    this.basicConfig = basicConfig;
    this.credentialsConfig = credentialsConfig;
    this.jmsConfig = jmsConfig;
    this.jmsMessageConverter = jmsMessageConverter;
  }

  @Override
  public List<Stage.ConfigIssue> init(Source.Context context) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    try {
      if (credentialsConfig.useCredentials) {
        connection = connectionFactory.createConnection(credentialsConfig.username.get(), credentialsConfig.password.get());
      } else {
        connection = connectionFactory.createConnection();
      }
    } catch (JMSException|StageException ex) {
      if (credentialsConfig.useCredentials) {
        issues.add(context.createConfigIssue(
            JmsGroups.JMS.name(),
            "jmsConfig.connectionFactory",
            JmsErrors.JMS_03,
            connectionFactory.getClass().getName(),
            ex.toString()
        ));
        LOG.info(Utils.format(JmsErrors.JMS_03.getMessage(), connectionFactory.getClass().getName(), ex.toString()), ex);
      } else {
        issues.add(context.createConfigIssue(JmsGroups.JMS.name(), "jmsConfig.connectionFactory", JmsErrors.JMS_02,
          connectionFactory.getClass().getName(), ex.toString()));
        LOG.info(Utils.format(JmsErrors.JMS_02.getMessage(), connectionFactory.getClass().getName(), ex.toString())
          , ex);
      }
    }
    if (issues.isEmpty()) {
      try {
        connection.start();
      } catch (JMSException ex) {
        issues.add(context.createConfigIssue(JmsGroups.JMS.name(), "jmsConfig.connectionFactory", JmsErrors.JMS_04,
          ex.toString()));
        LOG.info(Utils.format(JmsErrors.JMS_04.getMessage(), ex.toString()), ex);
      }
    }
    if (issues.isEmpty()) {
      try {
        session = connection.createSession(true, Session.SESSION_TRANSACTED);;
      } catch (JMSException ex) {
        issues.add(context.createConfigIssue(JmsGroups.JMS.name(), "jmsConfig.connectionFactory", JmsErrors.JMS_06,
          ex.toString()));
        LOG.info(Utils.format(JmsErrors.JMS_06.getMessage(), ex.toString()), ex);
      }
    }
    if (issues.isEmpty()) {
      try {
        switch (jmsConfig.destinationType) {
          case UNKNOWN:
            destination = (Destination) initialContext.lookup(jmsConfig.destinationName);
            break;
          case QUEUE:
            destination = session.createQueue(jmsConfig.destinationName);
            break;
          case TOPIC:
            destination = session.createTopic(jmsConfig.destinationName);
            break;
          default:
            throw new IllegalArgumentException(Utils.format("Unknown destination type: {}", jmsConfig.destinationType));
        }
     } catch (JMSException | NamingException ex) {
        issues.add(context.createConfigIssue(JmsGroups.JMS.name(), "jmsConfig.destinationName", JmsErrors.JMS_05,
          jmsConfig.destinationName, String.valueOf(ex)));
        LOG.info(Utils.format(JmsErrors.JMS_05.getMessage(), jmsConfig.destinationName,
          String.valueOf(ex)), ex);
      }
    }
    if (issues.isEmpty()) {
      try {
        String messageSelector = jmsConfig.messageSelector;
        if (messageSelector == null) {
          messageSelector = "";
        }
        messageSelector = messageSelector.trim();
        messageConsumer = session.createConsumer(destination, messageSelector.isEmpty() ? null: messageSelector);
      } catch (JMSException ex) {
        issues.add(context.createConfigIssue(JmsGroups.JMS.name(), "jmsConfig.connectionFactory", JmsErrors.JMS_11,
          ex.toString()));
        LOG.info(Utils.format(JmsErrors.JMS_11.getMessage(), ex.toString()), ex);
      }
    }
    return issues;
  }

  @Override
  public int take(BatchMaker batchMaker, Source.Context context, int batchSize, long messageIndex)
  throws StageException {
    long start = System.currentTimeMillis();
    int numMessagesConsumed = 0;
    while (System.currentTimeMillis() - start < basicConfig.maxWaitTime && numMessagesConsumed < batchSize) {
      if (IS_TRACE_ENABLED) {
        LOG.trace("Attempting to take up to '{}' messages", (batchSize - numMessagesConsumed));
      }
      try {
        Message message = messageConsumer.receive(POLL_INTERVAL);
        if (message != null) {
          if (IS_TRACE_ENABLED) {
            LOG.trace("Got message: {}", message);
          }
          String messageId = jmsConfig.destinationName + "::" + messageIndex;
          int consumed = jmsMessageConverter.convert(batchMaker, context, messageId, message);
          messageIndex += consumed;
          numMessagesConsumed += consumed;
        }
      } catch (JMSException ex) {
        throw new StageException(JmsErrors.JMS_07, ex.toString(), ex);
      }
    }
    return numMessagesConsumed;
  }

  @Override
  public void commit() throws StageException {
    try {
      session.commit();
    } catch (JMSException ex) {
      throw new StageException(JmsErrors.JMS_08, ex.toString(), ex);
    }
  }

  @Override
  public void rollback() throws StageException {
    try {
      session.rollback();
    } catch (JMSException ex) {
      throw new StageException(JmsErrors.JMS_09, ex.toString(), ex);
    }
  }

  @Override
  public void close() {
    if (session != null) {
      try {
        session.close();
      } catch (JMSException ex) {
        LOG.warn("Error closing session: " + ex, ex);
      }
    }
    if (connection != null) {
      try {
        connection.close();
      } catch (JMSException ex) {
        LOG.warn("Error closing connection: " + ex, ex);
      }
    }
  }
}
