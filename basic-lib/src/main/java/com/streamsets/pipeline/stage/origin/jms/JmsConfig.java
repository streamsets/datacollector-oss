/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.jms;

import com.streamsets.pipeline.api.ConfigDef;

public class JmsConfig {
  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "JMS Initial Context Factory",
    description = "ActiveMQ example: org.apache.activemq.jndi.ActiveMQInitialContextFactory",
    displayPosition = 10,
    group = "JMS"
  )
  public String initialContextFactory;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "JNDI Connection Factory",
    description = "ActiveMQ example: GenericConnectionFactory",
    displayPosition = 12,
    group = "JMS"
  )
  public String connectionFactory;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "JMS Provider URL",
    description = "ActiveMQ example: tcp://mqserver:61616",
    displayPosition = 14,
    group = "JMS"
  )
  public String providerURL;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "JMS Destination Name",
    description = "Queue or topic name",
    displayPosition = 16,
    group = "JMS"
  )
  public String destinationName;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "JMS Message Selector",
    displayPosition = 19,
    group = "JMS"
  )
  public String messageSelector;
}
