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
package com.streamsets.pipeline.stage.lib.aws;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.streamsets.pipeline.api.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class AWSUtil {
  public static final String AWS_CONFIG_BEAN = "awsConfig";

  public static AWSCredentialsProvider getCredentialsProvider(AWSConfig config) {
    AWSCredentialsProvider credentialsProvider;
    Properties sysEnvVars = System.getProperties();
    if (!config.awsAccessKeyId.isEmpty() && !config.awsSecretAccessKey.isEmpty()) {
      credentialsProvider = new StaticCredentialsProvider(
          new BasicAWSCredentials(config.awsAccessKeyId, config.awsSecretAccessKey)
      );
    } else if (sysEnvVars.containsKey("AWS_ACCESS_KEY_ID") && sysEnvVars.containsKey("AWS_SECRET_ACCESS_KEY")) {
      credentialsProvider = new DefaultAWSCredentialsProviderChain();
    } else {
      credentialsProvider = new InstanceProfileCredentialsProvider();
    }
    return credentialsProvider;
  }

  public static void renameAWSCredentialsConfigs(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for (Config config : configs) {
      switch (config.getName()) {
        case "accessKeyId":
        case "awsAccessKeyId":
          configsToRemove.add(config);
          configsToAdd.add(new Config(AWS_CONFIG_BEAN + ".awsAccessKeyId", config.getValue()));
          break;
        case "secretAccessKey":
        case "awsSecretAccessKey":
          configsToRemove.add(config);
          configsToAdd.add(new Config(AWS_CONFIG_BEAN + ".awsSecretAccessKey", config.getValue()));
          break;
        default:
          break;
      }
    }

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);
  }

}
