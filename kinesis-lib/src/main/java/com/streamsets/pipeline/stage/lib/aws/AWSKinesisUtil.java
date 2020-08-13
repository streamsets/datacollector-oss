/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.lib.aws;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.retry.RetryMode;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.lib.kinesis.AdditionalClientConfiguration;
import com.streamsets.pipeline.stage.lib.kinesis.Errors;
import com.streamsets.pipeline.stage.origin.kinesis.Groups;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil.KINESIS_CONFIG_BEAN;

public class AWSKinesisUtil {
  private static final Logger LOG = LoggerFactory.getLogger(AWSKinesisUtil.class);

  private static final int MILLIS = 1000;

  private AWSKinesisUtil() {}

  public static AWSCredentialsProvider getCredentialsProvider(AWSConfig config) throws StageException {
    AWSCredentialsProvider credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();
    if (config.credentialMode == AWSCredentialMode.WITH_CREDENTIALS) {
      if (!StringUtils.isEmpty(config.awsAccessKeyId.get()) && !StringUtils.isEmpty(config.awsSecretAccessKey.get())) {
        credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(config.awsAccessKeyId.get(),
            config.awsSecretAccessKey.get()
        ));
      }
    }
    return credentialsProvider;
  }

  public static ClientConfiguration getClientConfiguration(ProxyConfig config) throws StageException {
    ClientConfiguration clientConfig = new ClientConfiguration();

    clientConfig.setConnectionTimeout(config.connectionTimeout * MILLIS);
    clientConfig.setSocketTimeout(config.socketTimeout * MILLIS);
    clientConfig.withMaxErrorRetry(config.retryCount);

    // Optional proxy settings
    if (config.useProxy) {
      if (config.proxyHost != null && !config.proxyHost.isEmpty()) {
        clientConfig.setProxyHost(config.proxyHost);
        clientConfig.setProxyPort(config.proxyPort);

        if (config.proxyUser != null && !config.proxyUser.get().isEmpty()) {
          clientConfig.setProxyUsername(config.proxyUser.get());
        }

        if (config.proxyPassword != null) {
          clientConfig.setProxyPassword(config.proxyPassword.get());
        }
      }
    }
    return clientConfig;
  }

  public static void renameAWSCredentialsConfigs(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for (Config config : configs) {
      switch (config.getName()) {
        case "kinesisConfig.awsAccessKeyId":
          configsToRemove.add(config);
          configsToAdd.add(new Config("kinesisConfig.awsConfig.awsAccessKeyId", config.getValue()));
          break;
        case "kinesisConfig.awsSecretAccessKey":
          configsToRemove.add(config);
          configsToAdd.add(new Config("kinesisConfig.awsConfig.awsSecretAccessKey", config.getValue()));
          break;

        default:
          break;
      }
    }

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);
  }

  public static ClientConfiguration addAdditionalClientConfiguration(
      ClientConfiguration conf, Map<String, String> additionalConfiguration, List<Stage.ConfigIssue> issues,
      Stage.Context context) {
    for (Map.Entry<String, String> property : additionalConfiguration.entrySet()) {
      try {
        switch (AdditionalClientConfiguration.getName(property.getKey())) {
          case USER_AGENT_PREFIX:
            conf.setUserAgentPrefix(property.getValue());
            break;
          case USER_AGENT_SUFFIX:
            conf.setUserAgentSuffix(property.getValue());
            break;
          case MAX_CONNECTIONS:
            conf.setMaxConnections(Integer.parseInt(property.getValue()));
            break;
          case REQUEST_TIMEOUT:
            conf.setRequestTimeout(Integer.parseInt(property.getValue()));
            break;
          case CLIENT_EXECUTION_TIMEOUT:
            conf.setClientExecutionTimeout(Integer.parseInt(property.getValue()));
            break;
          case THROTTLE_RETRIES:
            conf.withThrottledRetries(Boolean.parseBoolean(property.getValue()));
            break;
          case CONNECTION_MAX_IDLE_MILLIS:
            conf.setConnectionMaxIdleMillis(Long.parseLong(property.getValue()));
            break;
          case VALIDATE_AFTER_INACTIVITY_MILLIS:
            conf.setValidateAfterInactivityMillis(Integer.parseInt(property.getValue()));
            break;
          case USE_EXPECT_CONTINUE:
            conf.setUseExpectContinue(Boolean.parseBoolean(property.getValue()));
            break;
          case MAX_CONSECUTIVE_RETRIES_BEFORE_THROTTLING:
            conf.setMaxConsecutiveRetriesBeforeThrottling(Integer.parseInt(property.getValue()));
            break;
          case RETRY_MODE:
            RetryMode retryMode = RetryMode.fromName(property.getValue());
            conf.setRetryMode(retryMode);
            break;
          default:
            LOG.error(Errors.KINESIS_21.getMessage(), property.getKey());
            break;
        }
      } catch (IllegalArgumentException ex) {
        LOG.error(Utils.format(Errors.KINESIS_25.getMessage(), ex.toString()), ex);
        issues.add(context.createConfigIssue(Groups.KINESIS.name(),
            KINESIS_CONFIG_BEAN + ".kinesisConsumerConfigs",
            Errors.KINESIS_25,
            ex.toString()
        ));
      }
    }
    return conf;
  }
}
