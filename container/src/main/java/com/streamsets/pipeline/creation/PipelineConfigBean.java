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
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.DeliveryGuaranteeChooserValues;
import com.streamsets.pipeline.config.ErrorHandlingChooserValues;
import com.streamsets.pipeline.config.ExecutionModeChooserValues;
import com.streamsets.pipeline.config.MemoryLimitExceeded;
import com.streamsets.pipeline.config.MemoryLimitExceededChooserValues;
import com.streamsets.pipeline.config.PipelineGroups;

import java.util.Collections;
import java.util.List;
import java.util.Map;

// we are using the annotation for reference purposes only.
// the annotation processor does not work on this maven project
// we have a hardcoded 'datacollector-resource-bundles.json' file in resources
@GenerateResourceBundle
@StageDef(version = 1, label = "Pipeline")
@ConfigGroups(PipelineGroups.class)
public class PipelineConfigBean implements Stage {

  public static final int VERSION = 1;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Execution Mode",
      defaultValue= "STANDALONE",
      displayPosition = 10
  )
  @ValueChooser(ExecutionModeChooserValues.class)
  public ExecutionMode executionMode;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="AT_LEAST_ONCE",
      label = "Delivery Guarantee",
      displayPosition = 20
  )
  @ValueChooser(DeliveryGuaranteeChooserValues.class)
  public DeliveryGuarantee deliveryGuarantee;

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


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Memory Limit (MB)",
      defaultValue = "${jvm:maxMemoryMB() * 0.65}",
      description = "Maximum memory in MB a pipeline will be allowed to " +
                    "consume. Maximum and minimum values are based on SDC Java heap size.",
      displayPosition = 40,
      min = 128,
      group = ""
  )
  public long memoryLimit;


  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Constants",
      displayPosition = 10,
      group = "CONSTANTS"
  )
  public Map<String, Object> constants;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Error Records",
      displayPosition = 10,
      group = "BAD_RECORDS"
  )
  @ValueChooser(ErrorHandlingChooserValues.class)
  public String badRecordsHandling;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Worker Memory (MB)",
      defaultValue = "1024",
      displayPosition = 10,
      group = "CLUSTER",
      dependsOn = "executionMode",
      triggeredByValue = "CLUSTER"
  )
  public long clusterSlaveMemory;


  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Worker Java Options",
    defaultValue = "-XX:PermSize=128M -XX:MaxPermSize=256M",
    displayPosition = 20,
    group = "CLUSTER",
    dependsOn = "executionMode",
    triggeredByValue = "CLUSTER"
  )
  public String clusterSlaveJavaOpts;


  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    label = "Kerberos Authentication",
    defaultValue = "false",
    displayPosition = 30,
    group = "CLUSTER",
    dependsOn = "executionMode",
    triggeredByValue = "CLUSTER"
  )
  public boolean clusterKerberos;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Kerberos Principal",
    displayPosition = 40,
    group = "CLUSTER",
    dependsOn = "clusterKerberos",
    triggeredByValue = "true"
  )
  public String kerberosPrincipal;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Kerberos Keytab (file)",
    displayPosition = 50,
    group = "CLUSTER",
    dependsOn = "clusterKerberos",
    triggeredByValue = "true"
  )
  public String kerberosKeytab;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.MAP,
    label = "Launcher ENV",
    description = "Sets additional environment variables for the cluster launcher",
    displayPosition = 60,
    group = "CLUSTER",
    dependsOn = "executionMode",
    triggeredByValue = "CLUSTER"
  )
  public Map clusterLauncherEnv;

  @Override
  public List<ConfigIssue> init(Info info, Context context) {
    return Collections.emptyList();
  }

  @Override
  public void destroy() {
  }

}
