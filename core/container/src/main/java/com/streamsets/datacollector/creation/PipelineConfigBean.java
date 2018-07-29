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

import com.streamsets.datacollector.config.AmazonEMRConfig;
import com.streamsets.datacollector.config.DeliveryGuaranteeChooserValues;
import com.streamsets.datacollector.config.ErrorHandlingChooserValues;
import com.streamsets.datacollector.config.ErrorRecordPolicy;
import com.streamsets.datacollector.config.ErrorRecordPolicyChooserValues;
import com.streamsets.datacollector.config.ExecutionModeChooserValues;
import com.streamsets.datacollector.config.LogLevel;
import com.streamsets.datacollector.config.LogLevelChooserValues;
import com.streamsets.datacollector.config.MemoryLimitExceeded;
import com.streamsets.datacollector.config.MemoryLimitExceededChooserValues;
import com.streamsets.datacollector.config.PipelineGroups;
import com.streamsets.datacollector.config.PipelineLifecycleStageChooserValues;
import com.streamsets.datacollector.config.PipelineState;
import com.streamsets.datacollector.config.PipelineStateChooserValues;
import com.streamsets.datacollector.config.PipelineTestStageChooserValues;
import com.streamsets.datacollector.config.PipelineWebhookConfig;
import com.streamsets.datacollector.config.StatsTargetChooserValues;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.MultiValueChooserModel;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;

import java.util.Collections;
import java.util.List;
import java.util.Map;

// we are using the annotation for reference purposes only.
// the annotation processor does not work on this maven project
// we have a hardcoded 'datacollector-resource-bundles.json' file in resources
@GenerateResourceBundle
@StageDef(
    version = PipelineConfigBean.VERSION,
    label = "Pipeline",
    upgrader = PipelineConfigUpgrader.class,
    onlineHelpRefUrl = "not applicable"
)
@ConfigGroups(PipelineGroups.class)
public class PipelineConfigBean implements Stage {

  public static final int VERSION = 10;

  public static final String DEFAULT_STATS_AGGREGATOR_LIBRARY_NAME = "streamsets-datacollector-basic-lib";

  public static final String DEFAULT_STATS_AGGREGATOR_STAGE_NAME =
      "com_streamsets_pipeline_stage_destination_devnull_StatsDpmDirectlyDTarget";

  public static final String DEFAULT_STATS_AGGREGATOR_STAGE_VERSION = "1";

  public static final String STATS_DPM_DIRECTLY_TARGET = DEFAULT_STATS_AGGREGATOR_LIBRARY_NAME + "::" +
      DEFAULT_STATS_AGGREGATOR_STAGE_NAME + "::" + DEFAULT_STATS_AGGREGATOR_STAGE_VERSION;

  public static final String STATS_AGGREGATOR_DEFAULT = "streamsets-datacollector-basic-lib" +
      "::com_streamsets_pipeline_stage_destination_devnull_StatsNullDTarget::1";

  private static final String TRASH_TARGET = "streamsets-datacollector-basic-lib" +
      "::com_streamsets_pipeline_stage_destination_devnull_ToErrorNullDTarget::1";

  public static final String DEFAULT_TEST_ORIGIN_LIBRARY_NAME = "streamsets-datacollector-dev-lib";

  public static final String DEFAULT_TEST_ORIGIN_STAGE_NAME =
      "com_streamsets_pipeline_stage_devtest_rawdata_RawDataDSource";

  public static final String DEFAULT_TEST_ORIGIN_STAGE_VERSION = "3";

  public static final String RAW_DATA_ORIGIN = DEFAULT_TEST_ORIGIN_LIBRARY_NAME + "::" +
      DEFAULT_TEST_ORIGIN_STAGE_NAME + "::" + DEFAULT_TEST_ORIGIN_STAGE_VERSION;

  public static final String EDGE_HTTP_URL_DEFAULT = "http://localhost:18633";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Execution Mode",
      defaultValue= "STANDALONE",
      displayPosition = 10
  )
  @ValueChooserModel(ExecutionModeChooserValues.class)
  public ExecutionMode executionMode;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Data Collector Edge URL",
      defaultValue = EDGE_HTTP_URL_DEFAULT,
      displayPosition = 15,
      dependsOn = "executionMode",
      triggeredByValue = {"EDGE"}
  )
  public String edgeHttpUrl = EDGE_HTTP_URL_DEFAULT;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="AT_LEAST_ONCE",
      label = "Delivery Guarantee",
      displayPosition = 20
  )
  @ValueChooserModel(DeliveryGuaranteeChooserValues.class)
  public DeliveryGuarantee deliveryGuarantee;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Test Origin",
      description = "Stage used for testing in preview mode.",
      defaultValue = RAW_DATA_ORIGIN,
      displayPosition = 21
  )
  @ValueChooserModel(PipelineTestStageChooserValues.class)
  public String testOriginStage;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Start Event",
      description = "Stage that should handle pipeline start event.",
      defaultValue = TRASH_TARGET,
      displayPosition = 23,
      dependsOn = "executionMode",
      triggeredByValue =  {"STANDALONE", "CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "CLUSTER_MESOS_STREAMING"}
  )
  @ValueChooserModel(PipelineLifecycleStageChooserValues.class)
  public String startEventStage;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Stop Event",
      description = "Stage that should handle pipeline stop event.",
      defaultValue = TRASH_TARGET,
      displayPosition = 26,
      dependsOn = "executionMode",
      triggeredByValue =  {"STANDALONE", "CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "CLUSTER_MESOS_STREAMING"}
  )
  @ValueChooserModel(PipelineLifecycleStageChooserValues.class)
  public String stopEventStage;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Retry Pipeline on Error",
      displayPosition = 30,
      dependsOn = "executionMode",
      triggeredByValue =  {"STANDALONE", "CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "CLUSTER_MESOS_STREAMING"}
  )
  public boolean shouldRetry;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "-1",
      label = "Retry Attempts",
      dependsOn = "shouldRetry",
      triggeredByValue = "true",
      description = "Max no of retries. To retry indefinitely, use -1. The wait time between retries starts at 15 seconds"
          + " and doubles until reaching 5 minutes.",
      displayPosition = 30
  )
  public int retryAttempts;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Max Pipeline Memory (MB)",
      defaultValue = "${jvm:maxMemoryMB() * 0.85}",
      description = "Maximum amount of memory the pipeline can use. Configure in relationship to the SDC Java heap " +
          "size. The default is 85% of heap and a value of 0 disables the limit.",
      displayPosition = 60,
      min = 0,
      dependsOn = "executionMode",
      triggeredByValue =  {"STANDALONE", "CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "CLUSTER_MESOS_STREAMING"}
  )
  public long memoryLimit;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="LOG",
      label = "On Memory Exceeded",
      description = "Behavior when the pipeline exceeds the memory limit. Tip: Configure an alert to indicate when the " +
          "memory use approaches the limit." ,
      displayPosition = 70,
      dependsOn = "executionMode",
      triggeredByValue =  {"STANDALONE", "CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "CLUSTER_MESOS_STREAMING"}
  )
  @ValueChooserModel(MemoryLimitExceededChooserValues.class)
  public MemoryLimitExceeded memoryLimitExceeded;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "[\"RUN_ERROR\", \"STOPPED\", \"FINISHED\"]",
      label = "Notify on Pipeline State Changes",
      description = "Notifies via email when pipeline gets to the specified states",
      displayPosition = 75,
      group = "NOTIFICATIONS",
      dependsOn = "executionMode",
      triggeredByValue =  {"STANDALONE", "CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "CLUSTER_MESOS_STREAMING"}
  )
  @MultiValueChooserModel(PipelineStateChooserValues.class)
  public List<PipelineState> notifyOnStates;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.LIST,
      defaultValue = "[]",
      label = "Email IDs",
      description = "Email Addresses",
      displayPosition = 76,
      group = "NOTIFICATIONS",
      dependsOn = "executionMode",
      triggeredByValue =  {"STANDALONE", "CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "CLUSTER_MESOS_STREAMING"}
  )
  public List<String> emailIDs;

  @ConfigDef(
      required = false,
      defaultValue = "{}",
      type = ConfigDef.Type.MAP,
      label = "Parameters",
      displayPosition = 80,
      group = "PARAMETERS"
  )
  public Map<String, Object> constants;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Error Records",
      displayPosition = 90,
      group = "BAD_RECORDS"
  )
  @ValueChooserModel(ErrorHandlingChooserValues.class)
  public String badRecordsHandling;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="ORIGINAL_RECORD",
      label = "Error Record Policy",
      description = "Determines which variation of the record is sent to error.",
      displayPosition = 93,
      group = "BAD_RECORDS"
  )
  @ValueChooserModel(ErrorRecordPolicyChooserValues.class)
  public ErrorRecordPolicy errorRecordPolicy = ErrorRecordPolicy.ORIGINAL_RECORD;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Statistics Aggregator",
      defaultValue = STATS_DPM_DIRECTLY_TARGET,
      displayPosition = 95,
      group = "STATS"
  )
  @ValueChooserModel(StatsTargetChooserValues.class)
  public String statsAggregatorStage = STATS_DPM_DIRECTLY_TARGET;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Worker Count",
      description = "Number of workers. 0 to start as many workers as Kafka partitions for topic.",
      defaultValue = "0",
      min = 0,
      displayPosition = 100,
      group = "CLUSTER",
      dependsOn = "executionMode",
      triggeredByValue = {"CLUSTER_YARN_STREAMING"}
  )
  public long workerCount;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Worker Memory (MB)",
      defaultValue = "2048",
      displayPosition = 150,
      group = "CLUSTER",
      dependsOn = "executionMode",
      triggeredByValue = {"CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "EMR_BATCH"}
  )
  public long clusterSlaveMemory;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Worker Java Options",
      defaultValue = "-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -Dlog4j.debug",
      description = "Add properties as needed. Changes to default settings are not recommended.",
      displayPosition = 110,
      group = "CLUSTER",
      dependsOn = "executionMode",
      triggeredByValue = {"CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "EMR_BATCH"}
  )
  public String clusterSlaveJavaOpts;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      defaultValue = "{}",
      label = "Launcher ENV",
      description = "Sets additional environment variables for the cluster launcher",
      displayPosition = 120,
      group = "CLUSTER",
      dependsOn = "executionMode",
      triggeredByValue = {"CLUSTER_BATCH", "CLUSTER_YARN_STREAMING"}
  )
  public Map<String, String> clusterLauncherEnv;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Mesos Dispatcher URL",
      description = "URL for service which launches Mesos framework",
      displayPosition = 130,
      group = "CLUSTER",
      dependsOn = "executionMode",
      triggeredByValue = {"CLUSTER_MESOS_STREAMING"}
  )
  public String mesosDispatcherURL;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "INFO",
      label = "Log level",
      displayPosition = 140,
      group = "CLUSTER",
      dependsOn = "executionMode",
      triggeredByValue = {"EMR_BATCH"}
  )
  @ValueChooserModel(LogLevelChooserValues.class)
  public LogLevel logLevel;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Checkpoint Configuration Directory",
      description = "An SDC resource directory or symbolic link with HDFS/S3 configuration files core-site.xml and hdfs-site.xml",
      displayPosition = 150,
      group = "CLUSTER",
      dependsOn = "executionMode",
      triggeredByValue = {"CLUSTER_MESOS_STREAMING"}
  )
  public String hdfsS3ConfDir;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "0",
      label = "Rate Limit (records / sec)",
      description = "Maximum number of records per second that should be accepted into the pipeline. " +
          "Rate is not limited if this is not set, or is set to 0",
      displayPosition = 180,
      dependsOn = "executionMode",
      triggeredByValue =  {"STANDALONE", "CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "CLUSTER_MESOS_STREAMING"}
  )
  public long rateLimit;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "0",
      label = "Max runners",
      description = "Maximum number of runners that should be created for this pipeline. Use 0 to not impose limit.",
      min = 0,
      displayPosition = 190,
      dependsOn = "executionMode",
      triggeredByValue =  {"STANDALONE", "CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "CLUSTER_MESOS_STREAMING"}
  )
  public int maxRunners = 0;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Create Failure Snapshot",
      description = "When selected and the pipeline execution fails with unrecoverable exception, SDC will attempt to create" +
          "partial snapshot with records that have not been processed yet.",
      dependencies = @Dependency(
          configName = "executionMode", triggeredByValues = "STANDALONE"
      ),
      displayPosition = 200
  )
  public boolean shouldCreateFailureSnapshot;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "60",
      label = "Runner Idle Time (sec)",
      description = "When pipeline runners are idle for at least this time, run an empty batch through the runner to" +
          " process any events or other time-driven functionality. Value -1 will disable this functionality completely.",
      dependencies = @Dependency(
          configName = "executionMode", triggeredByValues = "STANDALONE"
      ),
      displayPosition = 210
  )
  public long runnerIdleTIme = 60;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "[]",
      label = "Webhooks",
      description = "Webhooks",
      displayPosition = 210,
      group = "NOTIFICATIONS",
      dependsOn = "executionMode",
      triggeredByValue =  {"STANDALONE", "CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "CLUSTER_MESOS_STREAMING"}
  )
  @ListBeanModel
  public List<PipelineWebhookConfig> webhookConfigs = Collections.emptyList();

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      defaultValue = "{}",
      label = "Extra Spark Configuration",
      description = "Additional Spark Configuration to pass to the spark-submit script, the parameters will be passed " +
          "as --conf <key>=<value>",
      displayPosition = 220,
      group = "CLUSTER",
      dependsOn = "executionMode",
      triggeredByValue = {"CLUSTER_YARN_STREAMING"}
  )
  public Map<String, String> sparkConfigs;

  @ConfigDefBean
  public AmazonEMRConfig amazonEMRConfig;

  @Override
  public List<ConfigIssue> init(Info info, Context context) {
    return Collections.emptyList();
  }

  @Override
  public void destroy() {
  }

}
