/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.creation;

import com.streamsets.datacollector.config.DeliveryGuarantee;
import com.streamsets.datacollector.config.DeliveryGuaranteeChooserValues;
import com.streamsets.datacollector.config.ErrorHandlingChooserValues;
import com.streamsets.datacollector.config.ExecutionModeChooserValues;
import com.streamsets.datacollector.config.MemoryLimitExceeded;
import com.streamsets.datacollector.config.MemoryLimitExceededChooserValues;
import com.streamsets.datacollector.config.PipelineGroups;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;

import java.util.Collections;
import java.util.List;
import java.util.Map;

// we are using the annotation for reference purposes only.
// the annotation processor does not work on this maven project
// we have a hardcoded 'datacollector-resource-bundles.json' file in resources
@GenerateResourceBundle
@StageDef(version = PipelineConfigBean.VERSION, label = "Pipeline")
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
    type = ConfigDef.Type.NUMBER,
    label = "Max Pipeline Memory (MB)",
    defaultValue = "${jvm:maxMemoryMB() * 0.65}",
    description = "Maximum amount of memory the pipeline can use. Configure in relationship to the SDC Java heap " +
      "size. Default is 668.",
    displayPosition = 30,
    min = 128,
    group = ""
  )
  public long memoryLimit;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="STOP_PIPELINE",
      label = "On Memory Exceeded",
      description = "Behavior when the pipeline exceeds the memory limit. Tip: Configure an alert to indicate when the " +
        "memory use approaches the limit." ,
      displayPosition = 40,
      group = ""
  )
  @ValueChooser(MemoryLimitExceededChooserValues.class)
  public MemoryLimitExceeded memoryLimitExceeded;



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
    defaultValue = "-XX:PermSize=128M -XX:MaxPermSize=256M -Dlog4j.debug",
    description = "Add properties as needed. Changes to default settings are not recommended.",
    displayPosition = 20,
    group = "CLUSTER",
    dependsOn = "executionMode",
    triggeredByValue = "CLUSTER"
  )
  public String clusterSlaveJavaOpts;


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
