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

import com.streamsets.datacollector.config.ClusterConfig;
import com.streamsets.datacollector.config.DatabricksConfig;
import com.streamsets.datacollector.config.DeliveryGuaranteeChooserValues;
import com.streamsets.datacollector.config.ErrorHandlingChooserValues;
import com.streamsets.datacollector.config.ErrorRecordPolicy;
import com.streamsets.datacollector.config.ErrorRecordPolicyChooserValues;
import com.streamsets.datacollector.config.ExecutionModeChooserValues;
import com.streamsets.datacollector.config.LivyConfig;
import com.streamsets.datacollector.config.LogLevel;
import com.streamsets.datacollector.config.LogLevelChooserValues;
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
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.MultiValueChooserModel;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.googlecloud.DataProcCredentialsConfig;
import com.streamsets.pipeline.lib.googlecloud.GoogleCloudConfig;
import com.streamsets.pipeline.stage.common.emr.EMRClusterConnection;
import com.streamsets.transformer.config.AmazonEMRConfig;

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
    upgraderDef = "upgrader/PipelineConfigBeanUpgrader.yaml",
    onlineHelpRefUrl = "not applicable"
)
@HideConfigs({
    // hide the assume role configs until they can be implemented for connection catalog
    "sdcEmrConnection.awsConfig.isAssumeRole",
    "transformerEmrConnection.awsConfig.isAssumeRole"
})
@ConfigGroups(PipelineGroups.class)
public class PipelineConfigBean implements Stage {

  public static final int VERSION = 22;

  public static final String DEFAULT_STATS_AGGREGATOR_LIBRARY_NAME = "streamsets-datacollector-basic-lib";

  public static final String DEFAULT_STATS_AGGREGATOR_STAGE_NAME =
      "com_streamsets_pipeline_stage_destination_devnull_StatsDpmDirectlyDTarget";

  public static final String DEFAULT_STATS_AGGREGATOR_STAGE_VERSION = "1";

  public static final String STATS_DPM_DIRECTLY_TARGET = DEFAULT_STATS_AGGREGATOR_LIBRARY_NAME + "::" +
      DEFAULT_STATS_AGGREGATOR_STAGE_NAME + "::" + DEFAULT_STATS_AGGREGATOR_STAGE_VERSION;

  public static final String STREAMING_STATS_AGGREGATOR_LIBRARY_NAME = "streamsets-spark-basic-lib";

  public static final String STREAMING_STATS_DPM_DIRECTLY_TARGET = STREAMING_STATS_AGGREGATOR_LIBRARY_NAME + "::" +
      DEFAULT_STATS_AGGREGATOR_STAGE_NAME + "::" + DEFAULT_STATS_AGGREGATOR_STAGE_VERSION;

  public static final String STATS_AGGREGATOR_DEFAULT = "streamsets-datacollector-basic-lib" +
      "::com_streamsets_pipeline_stage_destination_devnull_StatsNullDTarget::1";

  public static final String TRASH_LIBRARY_NAME = "streamsets-datacollector-basic-lib";
  public static final String TRASH_STAGE_NAME = "com_streamsets_pipeline_stage_destination_devnull_ToErrorNullDTarget";
  public static final String TRASH_STAGE_VERSION = "1";
  private static final String TRASH_TARGET =  TRASH_LIBRARY_NAME + "::" + TRASH_STAGE_NAME + "::" + TRASH_STAGE_VERSION;

  public static final String DEFAULT_TEST_ORIGIN_LIBRARY_NAME = "streamsets-datacollector-dev-lib";

  public static final String DEFAULT_TEST_ORIGIN_STAGE_NAME =
      "com_streamsets_pipeline_stage_devtest_rawdata_RawDataDSource";

  public static final String DEFAULT_TEST_ORIGIN_STAGE_VERSION = "3";

  public static final String RAW_DATA_ORIGIN = DEFAULT_TEST_ORIGIN_LIBRARY_NAME + "::" +
      DEFAULT_TEST_ORIGIN_STAGE_NAME + "::" + DEFAULT_TEST_ORIGIN_STAGE_VERSION;

  public static final String EDGE_HTTP_URL_DEFAULT = "http://localhost:18633";

  public static final String DEFAULT_PREPROCESS_SCRIPT = "" +
      "/*\n" +
      "The following script define a method\n" +
      "that increments an integer by 1 \n" +
      "and registers it as a UDF with \n" +
      "the SparkSession, which can be accessed\n" +
      "using the variable named \"spark\":\n" +
      "def inc(i: Integer): Integer = {\n" +
      "  i + 1\n" +
      "}\n" +
      "spark.udf.register (\"inc\", inc _)\n" +
      "\n" +
      "*/";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Execution Mode",
      defaultValue= "STANDALONE",
      /*
       The display mode for executionMode here is kind of bogus and is ignored by the UI. This is because the field is
       re-used between SDC and Transformer and each product needs it on a different value. SDC considers this field
       as ADVANCED whereas Transformer as BASIC.
      */
      displayMode = ConfigDef.DisplayMode.BASIC,
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
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      triggeredByValue = {"EDGE"}
  )
  public String edgeHttpUrl = EDGE_HTTP_URL_DEFAULT;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="AT_LEAST_ONCE",
      label = "Delivery Guarantee",
      displayPosition = 20,
      dependsOn = "executionMode",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      triggeredByValue =  {"STANDALONE", "CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "CLUSTER_MESOS_STREAMING", "EDGE", "EMR_BATCH"}
  )
  @ValueChooserModel(DeliveryGuaranteeChooserValues.class)
  public DeliveryGuarantee deliveryGuarantee;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Test Origin",
      description = "Stage used for testing in preview mode.",
      defaultValue = RAW_DATA_ORIGIN,
      displayPosition = 21,
      dependsOn = "executionMode",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      triggeredByValue =  {"STANDALONE", "CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "CLUSTER_MESOS_STREAMING", "EDGE", "EMR_BATCH"}
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
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      triggeredByValue =  {"STANDALONE"}
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
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      triggeredByValue =  {"STANDALONE"}
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
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      triggeredByValue =  {"STANDALONE", "CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "CLUSTER_MESOS_STREAMING"}
  )
  public boolean shouldRetry;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "2000",
      label = "Trigger Interval (millis)",
      description = "Time interval between generation of batches",
      min = 1,
      dependencies = {
          @Dependency(configName = "executionMode", triggeredByValues = {"STREAMING"})
      },
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      displayPosition = 35
  )
  public long triggerInterval = 1; // default so tests don't wait forever

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "-1",
      label = "Retry Attempts",
      dependsOn = "shouldRetry",
      triggeredByValue = "true",
      description = "Max no of retries. To retry indefinitely, use -1. The wait time between retries starts at 15 seconds"
          + " and doubles until reaching 5 minutes.",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      displayPosition = 30
  )
  public int retryAttempts;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Enable Ludicrous Mode",
      description = "Ludicrous mode may significantly improve performance, but metrics will be limited",
      dependencies = {
          @Dependency(configName = "executionMode", triggeredByValues = {"BATCH", "STREAMING"})
      },
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      displayPosition = 40
  )
  public boolean ludicrousMode;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Collect Input Metrics",
      description = "Collects and displays input metrics. Can result in rereading data unless origins are configured to cache data",
      dependencies = {
          @Dependency(configName = "ludicrousMode", triggeredByValues = "true")
      },
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      displayPosition = 50
  )
  public boolean ludicrousModeInputCount;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Advanced Error Handling",
      description = "Reports the record that generates an error, when possible. Supported in single-origin pipelines",
      dependencies = {
          @Dependency(configName = "executionMode", triggeredByValues = {"BATCH", "STREAMING"})
      },
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      displayPosition = 60
  )
  public boolean advancedErrorHandling;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "[\"RUN_ERROR\", \"STOPPED\", \"FINISHED\"]",
      label = "Notify on Pipeline State Changes",
      description = "Notifies via email when pipeline gets to the specified states",
      displayPosition = 75,
      group = "NOTIFICATIONS",
      dependsOn = "executionMode",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      triggeredByValue =  {"STANDALONE", "CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "CLUSTER_MESOS_STREAMING", "BATCH", "STREAMING"}
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
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      triggeredByValue =  {"STANDALONE", "CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "CLUSTER_MESOS_STREAMING", "BATCH", "STREAMING"}
  )
  public List<String> emailIDs;

  @ConfigDef(
      required = false,
      defaultValue = "{}",
      type = ConfigDef.Type.MAP,
      label = "Parameters",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "PARAMETERS"
  )
  public Map<String, Object> constants;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Error Records",
      displayPosition = 90,
      defaultValue = TRASH_TARGET,
      group = "BAD_RECORDS",
      dependsOn = "executionMode",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      triggeredByValue =  {"STANDALONE", "CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "CLUSTER_MESOS_STREAMING", "EDGE", "EMR_BATCH"}
  )
  @ValueChooserModel(ErrorHandlingChooserValues.class)
  public String badRecordsHandling = TRASH_TARGET;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="ORIGINAL_RECORD",
      label = "Error Record Policy",
      description = "Determines which variation of the record is sent to error.",
      displayPosition = 93,
      group = "BAD_RECORDS",
      dependsOn = "executionMode",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      triggeredByValue =  {"STANDALONE", "CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "CLUSTER_MESOS_STREAMING", "EDGE", "EMR_BATCH"}
  )
  @ValueChooserModel(ErrorRecordPolicyChooserValues.class)
  public ErrorRecordPolicy errorRecordPolicy = ErrorRecordPolicy.ORIGINAL_RECORD;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Statistics Aggregator",
      defaultValue = STATS_DPM_DIRECTLY_TARGET,
      displayPosition = 95,
      group = "STATS",
      dependsOn = "executionMode",
      triggeredByValue =  {"STANDALONE", "CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "CLUSTER_MESOS_STREAMING", "EDGE", "EMR_BATCH", "BATCH", "STREAMING"}
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
      group = PipelineGroups.CLUSTER_GROUP_NAME,
      dependsOn = "executionMode",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      triggeredByValue = {"CLUSTER_YARN_STREAMING"}
  )
  public long workerCount;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Worker Memory (MB)",
      defaultValue = "2048",
      displayPosition = 150,
      group = PipelineGroups.CLUSTER_GROUP_NAME,
      dependsOn = "executionMode",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
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
      group = PipelineGroups.CLUSTER_GROUP_NAME,
      dependsOn = "executionMode",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
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
      group = PipelineGroups.CLUSTER_GROUP_NAME,
      dependsOn = "executionMode",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      triggeredByValue = {"CLUSTER_BATCH", "CLUSTER_YARN_STREAMING"}
  )
  public Map<String, String> clusterLauncherEnv;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Mesos Dispatcher URL",
      description = "URL for service which launches Mesos framework",
      displayPosition = 130,
      group = PipelineGroups.CLUSTER_GROUP_NAME,
      dependsOn = "executionMode",
      triggeredByValue = {"CLUSTER_MESOS_STREAMING"}
  )
  public String mesosDispatcherURL;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "INFO",
      label = "Log Level",
      description = "Log level to use for the launched application",
      displayPosition = 140,
      group = PipelineGroups.CLUSTER_GROUP_NAME,
      dependsOn = "executionMode",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      triggeredByValue = {"EMR_BATCH", "BATCH", "STREAMING"}
  )
  @ValueChooserModel(LogLevelChooserValues.class)
  public LogLevel logLevel;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Checkpoint Configuration Directory",
      description = "An SDC resource directory or symbolic link with HDFS/S3 configuration files core-site.xml and hdfs-site.xml",
      displayPosition = 150,
      group = PipelineGroups.CLUSTER_GROUP_NAME,
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
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      triggeredByValue =  {"STANDALONE", "CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "CLUSTER_MESOS_STREAMING"}
  )
  public long rateLimit;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "0",
      label = "Max Runners",
      description = "Maximum number of runners that should be created for this pipeline. Use 0 to not impose limit.",
      min = 0,
      displayPosition = 190,
      dependsOn = "executionMode",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      triggeredByValue =  {"STANDALONE", "CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "CLUSTER_MESOS_STREAMING"}
  )
  public int maxRunners = 0;

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
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      displayPosition = 200
  )
  public long runnerIdleTIme = 60;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Create Failure Snapshot",
      description = "Attempts to create a partial snapshot with unprocessed records if the pipeline fails",
      dependencies = @Dependency(
          configName = "executionMode", triggeredByValues = "STANDALONE"
      ),
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      displayPosition = 205
  )
  public boolean shouldCreateFailureSnapshot;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "[]",
      label = "Webhooks",
      description = "Webhooks",
      displayPosition = 210,
      group = "NOTIFICATIONS",
      dependsOn = "executionMode",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      triggeredByValue =  {"STANDALONE", "CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "CLUSTER_MESOS_STREAMING", "BATCH", "STREAMING"}
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
      group = PipelineGroups.CLUSTER_GROUP_NAME,
      dependsOn = "executionMode",
      triggeredByValue = {"CLUSTER_BATCH", "CLUSTER_YARN_STREAMING", "BATCH", "STREAMING"}
  )
  public Map<String, String> sparkConfigs;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      mode = ConfigDef.Mode.SCALA,
      defaultValue = DEFAULT_PREPROCESS_SCRIPT,
      label = "Preprocessing Script",
      description = "Scala script to run on the driver before starting the pipeline. " +
          "Can be used to register user defined functions, etc. Use the 'spark' variable to access the Spark session",
      displayPosition = 10,
      group = "ADVANCED",
      dependsOn = "executionMode",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      triggeredByValue = {"BATCH", "STREAMING"}
  )
  public String preprocessScript;

  @ConfigDefBean
  public ClusterConfig clusterConfig = new ClusterConfig();

  @ConfigDefBean
  public DatabricksConfig databricksConfig;

  @ConfigDefBean
  public LivyConfig livyConfig;

  // this is a dummy field, and hidden because EMR cluster selection is not implemented in SDC (see hidden configs)
  // but for the connection field (below) to work properly, it must be defined
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      // we need to disallow connection selection for SDC EMR (see SDC-15722), but we also cannot hide this config,
      // because then the dependent field (sdcEmrConnection, below) will not show up, so the way this is accomplished
      // is by setting the type to a dummy value, so that the real connections won't be loaded
      connectionType = "NOT-IMPLEMENTED-SDC-EMR-CONNECTION",
      defaultValue = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL,
      label = "Connection",
      group = PipelineGroups.CLUSTER_GROUP_NAME,
      displayPosition = -500,
      dependsOn = "executionMode",
      triggeredByValue = "EMR_BATCH"
  )
  @ValueChooserModel(ConnectionDef.Constants.ConnectionChooserValues.class)
  public String sdcEmrConnectionSelection = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL;

  // we have to have two separate connection objects for Data Collector and Transformer here, because
  // our current framework doesn't allow expressing an OR condition, for the dependencies
  // the SDC version hinges on executionMode=EMR_BATCH
  @ConfigDefBean(
      dependencies = {
          @Dependency(
              configName = "sdcEmrConnectionSelection",
              triggeredByValues = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL
          )
      },
      groups = PipelineGroups.EMR_GROUP_NAME
  )
  public EMRClusterConnection sdcEmrConnection;

  // this is an SDC-specific config that is not part of the common connection class
  // because there is only one, keep as a single field rather than having a separate bean class for it
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Enable Debugging",
      description = "Enable console debugging in EMR",
      group = PipelineGroups.EMR_GROUP_NAME,
      displayPosition = 3310,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependencies = {
          @Dependency(configName = "executionMode", triggeredByValues = "EMR_BATCH"),
          @Dependency(configName = "sdcEmrConnection.provisionNewCluster", triggeredByValues = "true"),
          @Dependency(configName = "sdcEmrConnection.loggingEnabled", triggeredByValues = "true"),

      }
  )
  public boolean enableEMRDebugging;

  // whereas the Transformer version hinges on clusterConfig.clusterType=EMR
  // and only Transformer supports using EMR connection selection (see SDC-15722), so this selection field is visible
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = EMRClusterConnection.TYPE,
      defaultValue = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL,
      label = "Connection",
      group = PipelineGroups.CLUSTER_GROUP_NAME,
      displayPosition = -600,
      dependsOn = "clusterConfig.clusterType",
      triggeredByValue = "EMR"
  )
  @ValueChooserModel(ConnectionDef.Constants.ConnectionChooserValues.class)
  public String transformerEmrConnectionSelection = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL;

  @ConfigDefBean(
      dependencies = {
          @Dependency(
              configName = "transformerEmrConnectionSelection",
              triggeredByValues = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL
          )
      },
      groups = PipelineGroups.CLUSTER_GROUP_NAME
  )
  public EMRClusterConnection transformerEmrConnection;

  // we also have Transformer-specific configs that aren't part of the common connection class
  @ConfigDefBean
  public AmazonEMRConfig transformerEMRConfig;

  @ConfigDefBean(dependencies = {
    @Dependency(configName = "clusterConfig.clusterType", triggeredByValues = "DATAPROC")
  }, groups = "DATAPROC")
  // The dependency does not resolve correctly if this inside another bean, so adding it here.
  public DataProcCredentialsConfig googleCloudCredentialsConfig = new DataProcCredentialsConfig();

  @ConfigDefBean(dependencies = {
    @Dependency(configName = "clusterConfig.clusterType", triggeredByValues = "DATAPROC")
  })
  public GoogleCloudConfig googleCloudConfig;

  @Override
  public List<ConfigIssue> init(Info info, Context context) {
    return Collections.emptyList();
  }

  @Override
  public void destroy() {
  }

}
