/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.jms;


import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.CredentialsConfig;
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
  private final JmsConfig jmsConfig;
  private final JmsMessageConverter jmsMessageConverter;
  private Connection connection;
  private Session session;
  private Destination destination;
  private MessageConsumer messageConsumer;

  public JmsMessageConsumerImpl(InitialContext initialContext, ConnectionFactory connectionFactory,
                                BasicConfig basicConfig, CredentialsConfig credentialsConfig,
                                JmsConfig jmsConfig, JmsMessageConverter jmsMessageConverter) {
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
        connection = connectionFactory.createConnection(credentialsConfig.username, credentialsConfig.password);
      } else {
        connection = connectionFactory.createConnection();
      }
    } catch (JMSException ex) {
      if (credentialsConfig.useCredentials) {
        issues.add(context.createConfigIssue(JmsGroups.JMS.name(), "connectionFactory", JmsErrors.JMS_03,
          connectionFactory.getClass().getName(), credentialsConfig.username, ex.toString()));
        LOG.info(Utils.format(JmsErrors.JMS_03.getMessage(), connectionFactory.getClass().getName(),
          credentialsConfig.username, ex.toString()), ex);
      } else {
        issues.add(context.createConfigIssue(JmsGroups.JMS.name(), "connectionFactory", JmsErrors.JMS_02,
          connectionFactory.getClass().getName(), ex.toString()));
        LOG.info(Utils.format(JmsErrors.JMS_02.getMessage(), connectionFactory.getClass().getName(), ex.toString())
          , ex);
      }
    }
    if (issues.isEmpty()) {
      try {
        connection.start();
      } catch (JMSException ex) {
        issues.add(context.createConfigIssue(JmsGroups.JMS.name(), "connectionFactory", JmsErrors.JMS_04,
          ex.toString()));
        LOG.info(Utils.format(JmsErrors.JMS_04.getMessage(), ex.toString()), ex);
      }
    }
    if (issues.isEmpty()) {
      try {
        session = connection.createSession(true, Session.SESSION_TRANSACTED);;
      } catch (JMSException ex) {
        issues.add(context.createConfigIssue(JmsGroups.JMS.name(), "connectionFactory", JmsErrors.JMS_06,
          ex.toString()));
        LOG.info(Utils.format(JmsErrors.JMS_06.getMessage(), ex.toString()), ex);
      }
    }
    if (issues.isEmpty()) {
      try {
        destination = (Destination) initialContext.lookup(jmsConfig.destinationName);
      } catch (NamingException ex) {
        issues.add(context.createConfigIssue(JmsGroups.JMS.name(), "destinationName", JmsErrors.JMS_05,
          jmsConfig.destinationName, ex.toString()));
        LOG.info(Utils.format(JmsErrors.JMS_05.getMessage(), jmsConfig.destinationName, ex.toString()), ex);
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
        issues.add(context.createConfigIssue(JmsGroups.JMS.name(), "connectionFactory", JmsErrors.JMS_11,
          ex.toString()));
        LOG.info(Utils.format(JmsErrors.JMS_11.getMessage(), ex.toString()), ex);
      }
    }
    return issues;
  }

  @Override
  public int take(BatchMaker batchMaker, Source.Context context, int batchSize, String messageId)
  throws StageException {
    long start = System.currentTimeMillis();
    int numMessagesConsumed = 0;
    while (System.currentTimeMillis() - start < basicConfig.maxWaitTime && numMessagesConsumed < batchSize) {
      if (IS_TRACE_ENABLED) {
        LOG.trace("Attempting to take up to '{}' messages", (batchSize - numMessagesConsumed));
      }
      int tries = 0;
      try {
        Message message = messageConsumer.receive(POLL_INTERVAL);
        if (message != null) {
          if (IS_TRACE_ENABLED) {
            LOG.trace("Got message: {}", message);
          }
          numMessagesConsumed += jmsMessageConverter.convert(batchMaker, context, messageId, message);
        }
      } catch (JMSException ex) {
        if (tries++ >= jmsConfig.maxTries) {
          LOG.warn(JmsErrors.JMS_07.getMessage(), ex.toString(), ex);
        } else {
          throw new StageException(JmsErrors.JMS_07, ex.toString(), ex);
        }
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
