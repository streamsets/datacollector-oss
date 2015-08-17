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
    label = "JMS Initial Conext Factory",
    description = "e.g: org.apache.activemq.jndi.ActiveMQInitialContextFactory",
    displayPosition = 10,
    group = "JMS"
  )
  public String initialContextFactory;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "JNDI Connection Factory",
    displayPosition = 12,
    group = "JMS"
  )
  public String connectionFactory;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "JMS Provider URL",
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

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.NUMBER,
    label = "JMS Message Selector",
    displayPosition = 20,
    group = "JMS",
    min = 1,
    max = Integer.MAX_VALUE,
    defaultValue = "3"
  )
  public int maxTries;
}
