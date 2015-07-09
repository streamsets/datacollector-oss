/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.creation;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.DeliveryGuaranteeChooserValues;
import com.streamsets.pipeline.config.ErrorHandlingChooserValues;
import com.streamsets.pipeline.config.ExecutionModeChooserValues;
import com.streamsets.pipeline.config.MemoryLimitExceeded;
import com.streamsets.pipeline.config.MemoryLimitExceededChooserValues;
import com.streamsets.pipeline.config.PipelineGroups;

import java.util.Map;

//TODO: use this class as initialization bean for the pipeline configuration
//
// we are using the annotation for reference purposes only.
// the annotation processor does not work on this maven project
// we have a hardcoded 'datacollector-resource-bundles.json' file in resources
@GenerateResourceBundle
@ConfigGroups(PipelineGroups.class)
public abstract class PipelineConfigBean implements Stage {

  public static final String EXECUTION_MODE_CONFIG = "executionMode";
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Execution Mode",
      defaultValue= "STANDALONE",
      displayPosition = 10
  )
  @ValueChooser(ExecutionModeChooserValues.class)
  public ExecutionMode executionMode;


  public static final String DELIVERY_GUARANTEE_CONFIG = "deliveryGuarantee";
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="AT_LEAST_ONCE",
      label = "Delivery Guarantee",
      displayPosition = 20
  )
  @ValueChooser(DeliveryGuaranteeChooserValues.class)
  public DeliveryGuarantee deliveryGuarantee;


  public static final String MEMORY_LIMIT_EXCEEDED_CONFIG = "memoryLimitExceeded";
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="STOP_PIPELINE",
      label = "Memory Limit Exceeded",
      description = "Behavior when a pipeline has exceeded the " +
                    "memory limit. Use Metric Alerts to alert before this limit has been exceeded." ,
      displayPosition = 30,
      group = ""
  )
  @ValueChooser(MemoryLimitExceededChooserValues.class)
  public MemoryLimitExceeded memoryLimitExceeded;


  public static final String MEMORY_LIMIT_CONFIG = "memoryLimit";
  public static final String MEMORY_LIMIT_DEFAULT = "${jvm:maxMemoryMB() * 0.65}";
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Memory Limit (MB)",
      defaultValue = MEMORY_LIMIT_DEFAULT,
      description = "Maximum memory in MB a pipeline will be allowed to " +
                    "consume. Maximum and minimum values are based on SDC Java heap size.",
      displayPosition = 40,
      min = 128,
      group = ""
  )
  public long memoryLimit;


  public static final String CONSTANTS_CONFIG = "constants";
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MAP,
      label = "Constants",
      displayPosition = 10,
      group = "CONSTANTS"
  )
  public Map<String, Object> constants;


  public static final String ERROR_RECORDS_CONFIG = "badRecordsHandling";
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Error Records",
      displayPosition = 10,
      group = "BAD_RECORDS"
  )
  @ValueChooser(ErrorHandlingChooserValues.class)
  public String badRecordsHandling;


  public static final String CLUSTER_SLAVE_MEMORY_CONFIG = "clusterSlaveMemory";
  public static final String CLUSTER_SLAVE_MEMORY_DEFAULT = "1024";
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Worker Memory (MB)",
      defaultValue = "1024",
      displayPosition = 10,
      group = "CLUSTER",
      dependsOn = EXECUTION_MODE_CONFIG,
      triggeredByValue = "CLUSTER"
  )
  public long clusterSlaveMemory;


  public static final String CLUSTER_SLAVE_JAVA_OPTS_CONFIG = "clusterSlaveJavaOpts";
  public static final String CLUSTER_SLAVE_JAVA_OPTS_DEFAULT = "-XX:PermSize=128M -XX:MaxPermSize=256M";
  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Worker Java Options",
    defaultValue = CLUSTER_SLAVE_JAVA_OPTS_DEFAULT,
    displayPosition = 20,
    group = "CLUSTER",
    dependsOn = EXECUTION_MODE_CONFIG,
    triggeredByValue = "CLUSTER"
  )
  public String clusterSlaveJavaOpts;


  public static final String CLUSTER_KERBEROS_AUTH_CONFIG = "clusterKerberos";
  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    label = "Kerberos Authentication",
    defaultValue = "false",
    displayPosition = 30,
    group = "CLUSTER",
    dependsOn = EXECUTION_MODE_CONFIG,
    triggeredByValue = "CLUSTER"
  )
  public boolean clusterKerberos;

  public static final String CLUSTER_KERBEROS_PRINCIPAL_CONFIG = "kerberosPrincipal";
  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "Kerberos Principal",
    displayPosition = 40,
    group = "CLUSTER",
    dependsOn = CLUSTER_KERBEROS_AUTH_CONFIG,
    triggeredByValue = "true"
  )
  public String kerberosPrincipal;

  public static final String CLUSTER_KERBEROS_KEYTAB_CONFIG = "kerberosKeytab";
  @ConfigDef(required = false,
    type = ConfigDef.Type.STRING,
    label = "Kerberos Keytab (file)",
    displayPosition = 50,
    group = "CLUSTER",
    dependsOn = CLUSTER_KERBEROS_AUTH_CONFIG,
    triggeredByValue = "true"
  )
  public String kerberosKeytab;

  public static final String CLUSTER_LAUNCHER_ENV_CONFIG = "clusterLauncherEnv";
  @ConfigDef(
    required = false,
    type = ConfigDef.Type.MAP,
    label = "Launcher ENV",
    description = "Sets additional environment variables for the cluster launcher",
    displayPosition = 60,
    group = "CLUSTER",
    dependsOn = EXECUTION_MODE_CONFIG,
    triggeredByValue = "CLUSTER"
  )
  public Map clusterLauncherEnv;
}
