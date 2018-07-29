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
package com.streamsets.pipeline.lib.rabbitmq.common;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.rabbitmq.config.BaseRabbitConfigBean;
import com.streamsets.pipeline.lib.rabbitmq.config.Errors;
import com.streamsets.pipeline.lib.rabbitmq.config.Groups;
import com.streamsets.pipeline.lib.rabbitmq.config.RabbitExchangeConfigBean;
import com.streamsets.pipeline.stage.common.DataFormatConfig;
import com.streamsets.pipeline.stage.destination.rabbitmq.BasicPropertiesConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public final class RabbitUtil {

  private RabbitUtil() {}

  private static final Logger LOG = LoggerFactory.getLogger(RabbitUtil.class);
  private static final String RABBIT_DATA_FORMAT_CONFIG_PREFIX = "conf.dataFormatConfig.";


  public static void initRabbitStage(
      Stage.Context context,
      BaseRabbitConfigBean conf,
      DataFormat dataFormat,
      DataFormatConfig dataFormatConfig,
      RabbitCxnManager rabbitCxnManager,
      List<Stage.ConfigIssue> issues
  ) {
    conf.init(context, issues);

    if (!issues.isEmpty()) {
      return;
    }

    try {
      rabbitCxnManager.init(conf);

      initRabbitConf(rabbitCxnManager.getChannel(), conf);

      dataFormatConfig.init(
          context,
          dataFormat,
          Groups.RABBITMQ.name(),
          RABBIT_DATA_FORMAT_CONFIG_PREFIX,
          issues
      );
    } catch (IllegalArgumentException e) {
      LOG.error("Configuration Error", e);
      issues.add(context.createConfigIssue(
          Groups.RABBITMQ.name(),
          "conf.uri",
          Errors.RABBITMQ_03,
          e.toString()
      ));
    } catch (TimeoutException | IOException | StageException e) {
      // Connecting to RabbitMQ timed out or some other issue.
      LOG.error("Rabbit MQ issue ", e);
      String reason = (e.getCause() == null) ? e.toString() : e.getCause().toString() ;
      issues.add(context.createConfigIssue(
          Groups.RABBITMQ.name(),
          "conf.uri",
          Errors.RABBITMQ_01,
          reason
      ));
    }
  }


  public static ConnectionFactory createConnectionFactory(BaseRabbitConfigBean conf) throws StageException {
    RabbitCxnFactoryBuilder builder =
        new RabbitCxnFactoryBuilder(
            conf.uri,
            conf.credentialsConfig.username.get(),
            conf.credentialsConfig.password.get(),
            conf.rabbitmqProperties
        ).setConnectionTimeout(conf.advanced.connectionTimeout)
            .setAutomaticRecoveryEnabled(conf.advanced.automaticRecoveryEnabled)
            .setNetworkRecoveryInterval(conf.advanced.networkRecoveryInterval)
            .setHeartbeatInterval(conf.advanced.heartbeatInterval)
            .setHandshakeTimeout(conf.advanced.handshakeTimeout)
            .setShutdownTimeout(conf.advanced.shutdownTimeout)
            .setFrameMax(conf.advanced.frameMax)
            .setChannelMax(conf.advanced.channelMax);
    return builder.build();
  }

  public static void buildBasicProperties(
      BasicPropertiesConfig basicPropertiesConfig,
      Stage.Context context,
      AMQP.BasicProperties.Builder builder,
      List<Stage.ConfigIssue> issues
  ) {
    if (isNotNullOrEmpty(basicPropertiesConfig.appId)) {
      builder.appId(basicPropertiesConfig.appId);
    }

    if (isNotNullOrEmpty(basicPropertiesConfig.contentEncoding)) {
      builder.contentEncoding(basicPropertiesConfig.contentEncoding);
    }

    if (isNotNullOrEmpty(basicPropertiesConfig.contentType)) {
      builder.contentType(basicPropertiesConfig.contentType);
    }

    if (isNotNullOrEmpty(basicPropertiesConfig.correlationId)) {
      builder.correlationId(basicPropertiesConfig.correlationId);
    }

    builder.deliveryMode(basicPropertiesConfig.deliveryMode.getDeliveryMode());

    if (basicPropertiesConfig.expiration < 0) {
      LOG.error("Invalid Configuration value for AMQP Basic Properties Expiration", basicPropertiesConfig.expiration);
      issues.add(context.createConfigIssue(
          Groups.RABBITMQ.name(),
          "conf.basicPropertiesConfig.expiration",
          Errors.RABBITMQ_09,
          basicPropertiesConfig.expiration,
          "Expiration"
      ));
      return;
    }
    builder.expiration(String.valueOf(basicPropertiesConfig.expiration));

    if (basicPropertiesConfig.headers != null && !basicPropertiesConfig.headers.isEmpty()) {
      builder.headers(basicPropertiesConfig.headers);
    }

    if (isNotNullOrEmpty(basicPropertiesConfig.messageId)) {
      builder.messageId(basicPropertiesConfig.messageId);
    }

    builder.priority(basicPropertiesConfig.priority.getPriority());

    if (isNotNullOrEmpty(basicPropertiesConfig.replyTo)) {
      builder.replyTo(basicPropertiesConfig.replyTo);
    }

    if (!basicPropertiesConfig.setCurrentTime) {
      if (basicPropertiesConfig.timestamp < 0) {
        LOG.error("Invalid Configuration value for AMQP Basic Properties TimeStamp", basicPropertiesConfig.timestamp);
        issues.add(context.createConfigIssue(
            Groups.RABBITMQ.name(),
            "conf.basicPropertiesConfig.timestamp",
            Errors.RABBITMQ_09,
            basicPropertiesConfig.timestamp,
            "Time Stamp"
        ));
        return;
      }
      builder.timestamp(new Date(basicPropertiesConfig.timestamp));
    }

    if (isNotNullOrEmpty(basicPropertiesConfig.type)) {
      builder.type(basicPropertiesConfig.type);
    }

    if (isNotNullOrEmpty(basicPropertiesConfig.userId)) {
      builder.userId(basicPropertiesConfig.userId);
    }

  }

  //----------------------------------------------- Private Members ---------------------------------------------------

  private static void initRabbitConf(Channel channel, BaseRabbitConfigBean conf) throws IOException{
    // Channel is always bound to the default exchange. When specified, we must declare the exchange.
    for (RabbitExchangeConfigBean exchange : conf.exchanges) {
      channel.exchangeDeclare(
          exchange.name,
          exchange.type.getValue(),
          exchange.durable,
          exchange.autoDelete,
          exchange.declarationProperties
      );
    }

    channel.queueDeclare(
        conf.queue.name,
        conf.queue.durable,
        conf.queue.exclusive,
        conf.queue.autoDelete,
        conf.queue.properties
    );

    for (RabbitExchangeConfigBean exchange : conf.exchanges) {
      bindQueue(channel, conf, exchange);
    }
  }

  private static void bindQueue(
      Channel channel,
      BaseRabbitConfigBean conf,
      RabbitExchangeConfigBean exchange
  ) throws IOException {
    // If an exchange is specified, we need the routing key and then we bind it to the channel.
    // Note that routing key is ignored for Fanout and Headers (unsupported) type exchanges.
    String bindingKey = exchange.routingKey.isEmpty() ? conf.queue.name : exchange.routingKey;
    channel.queueBind(conf.queue.name, exchange.name, bindingKey, exchange.bindingProperties);
  }

  private static boolean isNotNullOrEmpty(String obj) {
    return obj != null && !obj.isEmpty();
  }

  /** ConnectionFactory Builder for Rabbit**/
  private static class RabbitCxnFactoryBuilder {
    ConnectionFactory factory = null;
    String uri = null;

    public RabbitCxnFactoryBuilder(
        String uri,
        String username,
        String password,
        Map<String, Object> rabbitmqProperties
    ) {
      this.factory = new ConnectionFactory();
      this.uri = uri;
      this.factory.setUsername(username);
      this.factory.setPassword(password);
      this.factory.setClientProperties(rabbitmqProperties);
    }

    public RabbitCxnFactoryBuilder setConnectionTimeout(int connectionTimeout) {
      factory.setConnectionTimeout(connectionTimeout);
      return this;
    }

    public RabbitCxnFactoryBuilder setAutomaticRecoveryEnabled(boolean automaticRecoveryEnabled) {
      factory.setAutomaticRecoveryEnabled(automaticRecoveryEnabled);
      return this;
    }

    public RabbitCxnFactoryBuilder setNetworkRecoveryInterval(int networkRecoveryInterval) {
      factory.setNetworkRecoveryInterval(networkRecoveryInterval);
      return this;
    }

    public RabbitCxnFactoryBuilder setHeartbeatInterval(int heartbeatInterval) {
      factory.setRequestedHeartbeat(heartbeatInterval);
      return this;
    }

    public RabbitCxnFactoryBuilder setHandshakeTimeout(int handshakeTimeout) {
      factory.setHandshakeTimeout(handshakeTimeout);
      return this;
    }

    public RabbitCxnFactoryBuilder setShutdownTimeout(int shutdownTimeout) {
      factory.setShutdownTimeout(shutdownTimeout);
      return this;
    }

    public RabbitCxnFactoryBuilder setFrameMax(int frameMax) {
      factory.setRequestedFrameMax(frameMax);
      return this;
    }

    public RabbitCxnFactoryBuilder setChannelMax(int channelMax) {
      factory.setRequestedChannelMax(channelMax);
      return this;
    }

    public ConnectionFactory build() {
      Utils.checkNotNull(factory.getUsername(), "username");
      Utils.checkNotNull(factory.getPassword(), "password");
      Utils.checkNotNull(this.uri, "uri");
      try {
        factory.setUri(uri);
      } catch (URISyntaxException | NoSuchAlgorithmException | KeyManagementException e) {
        LOG.error("URI invalid ", e);
        throw new IllegalArgumentException(e.getMessage(), e.getCause());
      }
      return factory;
    }
  }
}
