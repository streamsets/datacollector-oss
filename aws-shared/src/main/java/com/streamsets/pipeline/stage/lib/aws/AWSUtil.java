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
package com.streamsets.pipeline.stage.lib.aws;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.Tag;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.Stage;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class AWSUtil {
  private static final String WILDCARD_PATTERN = ".*[\\*\\?].*";
  private static final String USER_PRINCIPAL = "streamsets/principal";
  private static final int MILLIS = 1000;

  private AWSUtil() {}

  public static AWSCredentialsProvider getCredentialsProvider(AWSConfig config, Stage.Context context, Regions region) {
    AWSCredentialsProvider credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();
    final String accessKeyId = config.awsAccessKeyId != null ? config.awsAccessKeyId.get() : null;
    final String secretAccessKey = config.awsSecretAccessKey != null ? config.awsSecretAccessKey.get() : null;

    switch (config.credentialMode) {
      case WITH_CREDENTIALS:
        if (!StringUtils.isEmpty(accessKeyId) && !StringUtils.isEmpty(secretAccessKey)) {
          credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretAccessKey));
        }
        break;
      case WITH_IAM_ROLES:
        // Using the DefaultAWSCredentialsProviderChain
        break;
      case WITH_ANONYMOUS_CREDENTIALS:
        credentialsProvider = new AWSStaticCredentialsProvider(new AnonymousAWSCredentials());
        break;
    }

    if (config.isAssumeRole) {
      STSAssumeRoleSessionCredentialsProvider.Builder builder = new STSAssumeRoleSessionCredentialsProvider.Builder(
          config.roleARN.get(),
          config.roleSessionName.isEmpty() ? UUID.randomUUID().toString() : config.roleSessionName
      ).withRoleSessionDurationSeconds(config.sessionDuration)
       .withStsClient(AWSSecurityTokenServiceClientBuilder.standard().withCredentials(credentialsProvider).withRegion(region).build());

      if (config.setSessionTags) {
        builder.withSessionTags(Collections.singletonList(new Tag().withKey(USER_PRINCIPAL)
                                                                   .withValue(context.getUserContext().getUser())));
      }

      credentialsProvider = builder.build();
    }

    return credentialsProvider;
  }

  public static AWSCredentialsProvider getCredentialsProvider(
      AWSCredentialMode credentialMode, String accessKeyId, String secretAccessKey
  ) {
    AWSCredentialsProvider credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();
    if (credentialMode != null) {
      switch (credentialMode) {
        case WITH_CREDENTIALS:
          if (!StringUtils.isEmpty(accessKeyId) && !StringUtils.isEmpty(secretAccessKey)) {
            credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId,
                secretAccessKey
            ));
          }
          break;
        case WITH_IAM_ROLES:
          // Using the DefaultAWSCredentialsProviderChain
          break;
        case WITH_ANONYMOUS_CREDENTIALS:
          credentialsProvider = new AWSStaticCredentialsProvider(new AnonymousAWSCredentials());
          break;
      }
    }
    return credentialsProvider;
  }

  public static ClientConfiguration getClientConfiguration(ProxyConfig config) {
    ClientConfiguration clientConfig = new ClientConfiguration();

    clientConfig.setConnectionTimeout(config.connectionTimeout * MILLIS);
    clientConfig.setSocketTimeout(config.socketTimeout * MILLIS);
    clientConfig.withMaxErrorRetry(config.retryCount);

    // Optional proxy settings
    if (config.useProxy && config.proxyHost != null && !config.proxyHost.isEmpty()) {
      clientConfig.setProxyHost(config.proxyHost);
      clientConfig.setProxyPort(config.proxyPort);

      if (config.proxyUser != null && !config.proxyUser.get().isEmpty()) {
        clientConfig.setProxyUsername(config.proxyUser.get());
      }

      if (config.proxyPassword != null && !config.proxyPassword.get().isEmpty()) {
        clientConfig.setProxyPassword(config.proxyPassword.get());
      }

      if (config.proxyDomain != null && !config.proxyDomain.isEmpty()) {
        clientConfig.setProxyDomain(config.proxyDomain);
      }

      if (config.proxyWorkstation != null && !config.proxyWorkstation.isEmpty()) {
        clientConfig.setProxyWorkstation(config.proxyWorkstation);
      }
    }
    return clientConfig;
  }

  public static void renameAWSCredentialsConfigs(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for (Config config : configs) {
      switch (config.getName()) {
        case "s3ConfigBean.s3Config.accessKeyId":
          configsToRemove.add(config);
          configsToAdd.add(new Config("s3ConfigBean.s3Config.awsConfig.awsAccessKeyId", config.getValue()));
          break;
        case "s3ConfigBean.s3Config.secretAccessKey":
          configsToRemove.add(config);
          configsToAdd.add(new Config("s3ConfigBean.s3Config.awsConfig.awsSecretAccessKey", config.getValue()));
          break;

        case "s3TargetConfigBean.s3Config.accessKeyId":
          configsToRemove.add(config);
          configsToAdd.add(new Config("s3TargetConfigBean.s3Config.awsConfig.awsAccessKeyId", config.getValue()));
          break;
        case "s3TargetConfigBean.s3Config.secretAccessKey":
          configsToRemove.add(config);
          configsToAdd.add(new Config("s3TargetConfigBean.s3Config.awsConfig.awsSecretAccessKey", config.getValue()));
          break;

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

  public static String normalizePrefix(String prefix, String delimiter) {
    if (prefix != null) {
      // if prefix starts with delimiter, remove it
      if (prefix.startsWith(delimiter)) {
        prefix = prefix.substring(delimiter.length());
      }
      // if prefix does not end with delimiter, add one
      if (!prefix.isEmpty() && !prefix.endsWith(delimiter)) {
        prefix = prefix + delimiter;
      }
    }
    return prefix;
  }

  public static boolean containsWildcard(String key) {
    return key.matches(WILDCARD_PATTERN);
  }
}
