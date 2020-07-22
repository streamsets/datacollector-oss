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
package com.streamsets.datacollector.creation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.config.AmazonEMRConfig;
import com.streamsets.datacollector.config.DatabricksConfig;
import com.streamsets.datacollector.config.PipelineState;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.googlecloud.GoogleCloudConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class PipelineConfigUpgrader implements StageUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineConfigUpgrader.class);
  public static final String GOOGLE_CLOUD_CONFIG_PREFIX = "googleCloudConfig.";
  public static final String GOOGLE_CLOUD_CREDENTIALS_CONFIG_PREFIX = "googleCloudCredentialsConfig.";

  @Override
  public List<Config> upgrade(List<Config> configs, Context context) throws StageException {
    switch(context.getFromVersion()) {
      case 0:
        // nothing to do from 0 to 1
      case 1:
        upgradeV1ToV2(configs);
        // fall through
      case 2:
        upgradeV2ToV3(configs);
        // fall through
      case 3:
        upgradeV3ToV4(configs);
        // fall through
      case 4:
        upgradeV4ToV5(configs);
        // fall through
      case 5:
        upgradeV5ToV6(configs);
        // fall through
      case 6:
        upgradeV6ToV7(configs);
        // fall through
      case 7:
        upgradeV7ToV8(configs);
        // fall through
      case 8:
        upgradeV8ToV9(configs);
        // fall through
      case 9:
        upgradeV9ToV10(configs);
        // fall through
      case 10:
        upgradeV10ToV11(configs);
        // fall through
      case 11:
        upgradeV11ToV12(configs);
        // fall through
      case 12:
        upgradeV12ToV13(configs);
        // fall through
      case 13:
        upgradeV13ToV14(configs);
        // fall through
      case 14:
        upgradeV14ToV15(configs);
        // fall through
      case 15:
        upgradeV15ToV16(configs);
        // fall through
      case 16:
        upgradeV16ToV17(configs);
        // fall through
      case 17:
        upgradeV17ToV18(configs);
        // fall through
      case 18:
        upgradeV18ToV19(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", context.getFromVersion()));
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    boolean found = false;
    for (Config config : configs) {
      if (config.getName().equals("executionMode")) {
        found = true;
      }
    }

    if(!found) {
      configs.add(new Config("executionMode", ExecutionMode.STANDALONE));
    }
  }

  private void upgradeV3ToV4(List<Config> configs) {
    boolean found = false;
    int index = 0;
    String sourceName = null;
    for (int i = 0; i < configs.size(); i++) {
      Config config = configs.get(i);
      if (config.getName().equals("executionMode")) {
        if (config.getValue().equals("CLUSTER")) {
          found = true;
          index = i;
        }
      } else if (config.getName().equals("sourceName")) {
        sourceName = config.getValue().toString();
      }
    }
    if (found) {
      configs.remove(index);
      Utils.checkNotNull(sourceName, "Source stage name cannot be null");
      configs.add(new Config(
          "executionMode",
          (sourceName.contains("ClusterHdfsDSource")) ?
              ExecutionMode.CLUSTER_BATCH : ExecutionMode.CLUSTER_YARN_STREAMING
      ));
    }
  }

  private void upgradeV2ToV3(List<Config> configs) {
    configs.add(new Config("shouldRetry", false));
    configs.add(new Config("retryAttempts", -1));
    configs.add(new Config("notifyOnStates",
        ImmutableList.of(PipelineState.RUN_ERROR, PipelineState.STOPPED, PipelineState.FINISHED)));
    configs.add(new Config("emailIDs", Collections.emptyList()));
  }

  private void upgradeV4ToV5(List<Config> configs) {
    configs.add(new Config("statsAggregatorStage", null));
  }

  private void upgradeV5ToV6(List<Config> configs) {
    configs.add(new Config("webhookConfigs", Collections.emptyList()));
  }

  private void upgradeV6ToV7(List<Config> configs) {
    configs.add(new Config("workerCount", 0));
  }

  private void upgradeV7ToV8(List<Config> configs) {
    boolean isClusterExecutionMode = isPipelineClusterMode(configs);
    if (isClusterExecutionMode) {
      Config statsAggregatorStageConfig = getStatsAggregatorStageConfig(configs);
      if (statsAggregatorStageConfig != null) {
        String statsAggregatorStage = (String) statsAggregatorStageConfig.getValue();
        if (statsAggregatorStage != null &&
            statsAggregatorStage.contains(PipelineConfigBean.STATS_DPM_DIRECTLY_TARGET)) {
          LOG.warn(
              "Cluster Pipeline Stats Aggregator is set to {} from {}",
              PipelineConfigBean.STATS_AGGREGATOR_DEFAULT,
              PipelineConfigBean.STATS_DPM_DIRECTLY_TARGET
          );
          configs.remove(statsAggregatorStageConfig);
          configs.add(new Config("statsAggregatorStage", PipelineConfigBean.STATS_AGGREGATOR_DEFAULT));
        }
      }
    }
  }

  public static boolean isPipelineClusterMode(List<Config> configs) {
    Set<String> clusterExecutionModes = Arrays.stream(ExecutionMode.values())
        .filter(executionMode -> executionMode.name().contains("CLUSTER"))
        .map(ExecutionMode::name)
        .collect(Collectors.toSet());
    return configs.stream()
        .anyMatch(config ->
            config.getName().equals("executionMode") && clusterExecutionModes.contains(config.getValue().toString())
        );
  }

  private static Config getStatsAggregatorStageConfig(List<Config> configs) {
    List<Config> statsAggregatorConfigList = configs.stream()
        .filter(config -> config.getName().equals("statsAggregatorStage"))
        .collect(Collectors.toList());
    return (!statsAggregatorConfigList.isEmpty())? statsAggregatorConfigList.get(0): null;
  }

  private void upgradeV8ToV9(List<Config> configs) {
    Config edgeHttpUrlConfig = configs.stream()
        .filter(config -> config.getName().equals("edgeHttpUrl"))
        .findFirst()
        .orElse(null);
    if (edgeHttpUrlConfig == null) {
      configs.add(new Config("edgeHttpUrl", PipelineConfigBean.EDGE_HTTP_URL_DEFAULT));
    }
  }

  private void upgradeV9ToV10(List<Config> configs) {
    Config edgeHttpUrlConfig = configs.stream()
        .filter(config -> config.getName().equals("testOriginStage"))
        .findFirst()
        .orElse(null);
    if (edgeHttpUrlConfig == null) {
      configs.add(new Config("testOriginStage", PipelineConfigBean.RAW_DATA_ORIGIN));
    }
    addAmazonEmrConfigs(configs);
  }

  private void addAmazonEmrConfigs(List<Config> configs) {
    String amazonEmrConfigPrefix = "amazonEMRConfig.";
    addEmrConfigs(configs, amazonEmrConfigPrefix);
    configs.add(new Config(amazonEmrConfigPrefix + AmazonEMRConfig.ENABLE_EMR_DEBUGGING, true));
    configs.add(new Config("logLevel", "INFO"));
  }

  private final static Set<String> PROPERTIES_TO_CHECK_FOR_CUSTOM =
      ImmutableSet.of(
          "amazonEMRConfig." + AmazonEMRConfig.USER_REGION,
          "amazonEMRConfig." + AmazonEMRConfig.MASTER_INSTANCE_TYPE,
          "amazonEMRConfig." + AmazonEMRConfig.SLAVE_INSTANCE_TYPE
      );

  private void upgradeV10ToV11(List<Config> configs) {
    for (int i = 0; i < configs.size(); i++) {
      if (PROPERTIES_TO_CHECK_FOR_CUSTOM.contains(configs.get(i).getName())) {
        if ("CUSTOM".equals(configs.get(i).getValue())) {
          configs.set(i, new Config(configs.get(i).getName(), "OTHER"));
        }
      }
    }
  }

  private void upgradeV11ToV12(List<Config> configs) {
    List<Config> memoryLimits = configs.stream()
      .filter(config -> config.getName().startsWith("memoryLimit"))
      .collect(Collectors.toList());

    configs.removeAll(memoryLimits);
  }

  private void upgradeV12ToV13(List<Config> configs) {
    addClusterConfigs(configs);
    addDatabricksConfigs(configs);
    addLivyConfigs(configs);
  }

  private void addClusterConfigs(List<Config> configs) {
    configs.add(new Config("clusterConfig.clusterType", "LOCAL"));
    configs.add(new Config("clusterConfig.sparkMasterUrl", "local[*]"));
    configs.add(new Config("clusterConfig.deployMode", "CLIENT"));
    configs.add(new Config("clusterConfig.hadoopUserName", "hdfs"));
    configs.add(new Config("clusterConfig.stagingDir", "/streamsets"));
  }

  private void addDatabricksConfigs(List<Config> configs) {
    configs.add(new Config("databricksConfig.baseUrl", null));
    configs.add(new Config("databricksConfig.credentialType", null));
    configs.add(new Config("databricksConfig.username", null));
    configs.add(new Config("databricksConfig.password", null));
    configs.add(new Config("databricksConfig.token", null));
    configs.add(new Config("databricksConfig.clusterConfig", DatabricksConfig.DEFAULT_CLUSTER_CONFIG));
  }

  private void addLivyConfigs(List<Config> configs) {
    configs.add(new Config("livyConfig.baseUrl", null));
    configs.add(new Config("livyConfig.username", null));
    configs.add(new Config("livyConfig.password", null));
  }

  private void upgradeV13ToV14(List<Config> configs) {
    configs.add(new Config("databricksConfig.provisionNewCluster", true));
    configs.add(new Config("databricksConfig.clusterId", null));
    configs.add(new Config("databricksConfig.terminateCluster", false));
  }

  private void upgradeV14ToV15(List<Config> configs) {
    configs.add(new Config("ludicrousMode", false));
    configs.add(new Config("ludicrousModeInputCount", false));
    configs.add(new Config("advancedErrorHandling", false));
    configs.add(new Config("triggerInterval", 2000));
  }

  private void upgradeV15ToV16(List<Config> configs) {
    configs.add(new Config("preprocessScript", ""));
  }

  private void upgradeV16ToV17(List<Config> configs) {
    String amazonEmrConfigPrefix = "transformerEMRConfig.";
    addEmrConfigs(configs, amazonEmrConfigPrefix);
    configs.add(new Config(amazonEmrConfigPrefix + "useIAMRoles", false));
    configs.add(new Config("clusterConfig.callbackUrl", null));
  }

  private void upgradeV17ToV18(List<Config> configs) {
    configs.add(new Config("transformerEMRConfig.serviceAccessSecurityGroup", null));
  }

  private void upgradeV18ToV19(List<Config> configs) {
    addDataprocConfigs(configs);
  }

  private void addEmrConfigs(List<Config> configs, String amazonEmrConfigPrefix) {
    configs.add(new Config(amazonEmrConfigPrefix + AmazonEMRConfig.USER_REGION, null));
    configs.add(new Config(amazonEmrConfigPrefix + AmazonEMRConfig.USER_REGION_CUSTOM, null));
    configs.add(new Config(amazonEmrConfigPrefix + AmazonEMRConfig.S3_STAGING_URI, null));
    configs.add(new Config(amazonEmrConfigPrefix + AmazonEMRConfig.CLUSTER_PREFIX, null));
    configs.add(new Config(amazonEmrConfigPrefix + AmazonEMRConfig.CLUSTER_ID, null));
    configs.add(new Config(amazonEmrConfigPrefix + AmazonEMRConfig.TERMINATE_CLUSTER, false));
    configs.add(new Config(amazonEmrConfigPrefix + AmazonEMRConfig.S3_LOG_URI, null));
    configs.add(new Config(amazonEmrConfigPrefix + AmazonEMRConfig.SERVICE_ROLE, AmazonEMRConfig.SERVICE_ROLE_DEFAULT));
    configs.add(new Config(
      amazonEmrConfigPrefix + AmazonEMRConfig.JOB_FLOW_ROLE,
      AmazonEMRConfig.JOB_FLOW_ROLE_DEFAULT
    ));
    configs.add(new Config(amazonEmrConfigPrefix + AmazonEMRConfig.VISIBLE_TO_ALL_USERS, true));
    configs.add(new Config(amazonEmrConfigPrefix + AmazonEMRConfig.LOGGING_ENABLED, true));
    configs.add(new Config(amazonEmrConfigPrefix + AmazonEMRConfig.EC2_SUBNET_ID, null));
    configs.add(new Config(amazonEmrConfigPrefix + AmazonEMRConfig.MASTER_SECURITY_GROUP, null));
    configs.add(new Config(amazonEmrConfigPrefix + AmazonEMRConfig.SLAVE_SECURITY_GROUP, null));
    configs.add(new Config(amazonEmrConfigPrefix + AmazonEMRConfig.INSTANCE_COUNT, 2));
    configs.add(new Config(amazonEmrConfigPrefix + AmazonEMRConfig.MASTER_INSTANCE_TYPE, null));
    configs.add(new Config(amazonEmrConfigPrefix + AmazonEMRConfig.SLAVE_INSTANCE_TYPE, null));
    configs.add(new Config(amazonEmrConfigPrefix + AmazonEMRConfig.MASTER_INSTANCE_TYPE_CUSTOM, null));
    configs.add(new Config(amazonEmrConfigPrefix + AmazonEMRConfig.SLAVE_INSTANCE_TYPE_CUSTOM, null));
    configs.add(new Config(amazonEmrConfigPrefix + AmazonEMRConfig.ACCESS_KEY, null));
    configs.add(new Config(amazonEmrConfigPrefix + AmazonEMRConfig.SECRET_KEY, null));
    configs.add(new Config(amazonEmrConfigPrefix + AmazonEMRConfig.PROVISION_NEW_CLUSTER, false));
  }

  private static void addDataprocConfigs(List<Config> configs) {
    addGCloudConfig(configs,"region", null);
    addGCloudConfig(configs, "customRegion", null);
    addGCloudConfig(configs, "gcsStagingUri", null);
    addGCloudConfig(configs, "create", false);
    addGCloudConfig(configs, "clusterPrefix", null);
    addGCloudConfig(configs, "version", GoogleCloudConfig.DATAPROC_IMAGE_VERSION_DEFAULT);
    addGCloudConfig(configs, "masterType", null);
    addGCloudConfig(configs, "workerType", null);
    addGCloudConfig(configs, "networkType", null);
    addGCloudConfig(configs, "network", null);
    addGCloudConfig(configs, "subnet", null);
    addGCloudConfig(configs, "tags", new ArrayList<String>());
    addGCloudConfig(configs, "workerCount", 2);
    addGCloudConfig(configs, "clusterName", null);
    addGCloudConfig(configs, "terminate", false);

    addGCloudCredentialConfig(configs, "projectId");
    addGCloudCredentialConfig(configs, "credentialsProvider");
    addGCloudCredentialConfig(configs, "path");
    addGCloudCredentialConfig(configs, "credentialsFileContent");

  }

  private static void addGCloudConfig(List<Config> configs, String key, Object value) {
    configs.add(new Config(GOOGLE_CLOUD_CONFIG_PREFIX + key, value));
  }

  private static void addGCloudCredentialConfig(List<Config> configs, String key) {
    configs.add(new Config(GOOGLE_CLOUD_CREDENTIALS_CONFIG_PREFIX + key, null));
  }

}
