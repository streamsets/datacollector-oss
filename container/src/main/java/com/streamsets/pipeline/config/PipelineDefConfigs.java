/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;


import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooser;

import java.util.Map;

//Dummy stage that is used to produce the resource bundle for the pipeline definition configs
//
// we are using the annotation for reference purposes only.
// the annotation processor does not work on this maven project
// we have a hardcoded 'datacollector-resource-bundles.json' file in resources
@GenerateResourceBundle
public abstract class PipelineDefConfigs implements Stage {

  public enum Groups implements Label {
    CONSTANTS("Constants"),
    BAD_RECORDS("Error Records"),
    CLUSTER("Cluster"),
    ;

    private final String label;

    Groups(String label) {
      this.label = label;
    }

    @Override
    public String getLabel() {
      return label;
    }
  }

  public static final String DELIVERY_GUARANTEE_CONFIG = "deliveryGuarantee";
  public static final String DELIVERY_GUARANTEE_LABEL = "Delivery Guarantee";
  public static final String DELIVERY_GUARANTEE_DESCRIPTION = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = DELIVERY_GUARANTEE_LABEL,
      description = DELIVERY_GUARANTEE_DESCRIPTION,
      displayPosition = 0,
      group = ""
  )
  @ValueChooser(DeliveryGuaranteeChooserValues.class)
  public DeliveryGuarantee deliveryGuarantee;

  public static final String EXECUTION_MODE_CONFIG = "executionMode";
  public static final String EXECUTION_MODE_LABEL = "Execution Mode";
  public static final String EXECUTION_MODE_DESCRIPTION = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = EXECUTION_MODE_LABEL,
      description = EXECUTION_MODE_DESCRIPTION,
      displayPosition = 0,
      group = ""
  )
  @ValueChooser(ExecutionModeChooserValues.class)
  public ExecutionMode executionMode;

  public static final String ERROR_RECORDS_CONFIG = "badRecordsHandling";
  public static final String ERROR_RECORDS_LABEL = "Error Records";
  public static final String ERROR_RECORDS_DESCRIPTION = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = ERROR_RECORDS_LABEL,
      description = ERROR_RECORDS_DESCRIPTION,
      displayPosition = 0,
      group = ""
  )
  @ValueChooser(ErrorHandlingChooserValues.class)
  public String badRecordsHandling;

  public static final String CONSTANTS_CONFIG = "constants";
  public static final String CONSTANTS_LABEL = "Constants";
  public static final String CONSTANTS_DESCRIPTION = "";

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MAP,
    defaultValue="",
    label = CONSTANTS_LABEL,
    description = CONSTANTS_DESCRIPTION,
    displayPosition = 0,
    group = ""
  )
  public Map<String, Object> constants;

  public static final String MEMORY_LIMIT_CONFIG = "memoryLimit";
  public static final String MEMORY_LIMIT_LABEL = "Memory Limit (MB)";
  public static final String MEMORY_LIMIT_DESCRIPTION_BASE = "Maximum memory in MB a pipeline will be allowed to " +
    "consume. Maximum and minimum values are based on SDC heap size.";
  public static final String MEMORY_LIMIT_DESCRIPTION;
  /*
   * Note that these values are not used by the localizer and thus do not need to be on
   * the ConfigDef below.
   */
  public static final Long MEMORY_LIMIT_DEFAULT;
  public static final Long MEMORY_LIMIT_MAX;
  public static final Long MEMORY_LIMIT_MIN;


  static {
    double maxMemoryMiB = Runtime.getRuntime().maxMemory() / 1000.0d / 1000.0d;
    MEMORY_LIMIT_MAX = (long)(maxMemoryMiB * 0.70d);
    MEMORY_LIMIT_MIN = (long)(maxMemoryMiB * 0.10d);
    MEMORY_LIMIT_DESCRIPTION = MEMORY_LIMIT_DESCRIPTION_BASE + " Max: " + MEMORY_LIMIT_MAX + ", Min: " +
      MEMORY_LIMIT_MIN;
    MEMORY_LIMIT_DEFAULT = MEMORY_LIMIT_MAX;
  }

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "",
    label = MEMORY_LIMIT_LABEL,
    description = MEMORY_LIMIT_DESCRIPTION_BASE,
    displayPosition = 20,
    group = ""
  )
  public long memoryLimit;

  public static final String MEMORY_LIMIT_EXCEEDED_CONFIG = "memoryLimitExceeded";
  public static final String MEMORY_LIMIT_EXCEEDED_LABEL = "Memory Limit Exceeded";
  public static final String MEMORY_LIMIT_EXCEEDED_DESCRIPTION = "Behavior when a pipeline has exceeded the " +
    "memory limit. Use Metric Alerts to alert before this limit has been exceeded.";
  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue="",
    label = MEMORY_LIMIT_EXCEEDED_LABEL,
    description = MEMORY_LIMIT_EXCEEDED_DESCRIPTION ,
    displayPosition = 30,
    group = ""
  )
  @ValueChooser(MemoryLimitExceededChooserValues.class)
  public MemoryLimitExceeded memoryLimitExceeded;


  public static final String CLUSTER_SLAVE_MEMORY_CONFIG = "clusterSlaveMemory";
  public static final String CLUSTER_SLAVE_MEMORY_LABEL = "Slave Heap (MB)";
  public static final String CLUSTER_SLAVE_MEMORY_DEFAULT = "1024";
  public static final String CLUSTER_SLAVE_MEMORY_DESCRIPTION = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = CLUSTER_SLAVE_MEMORY_LABEL,
      defaultValue = CLUSTER_SLAVE_MEMORY_DEFAULT,
      description = CLUSTER_SLAVE_MEMORY_DESCRIPTION,
      displayPosition = 10,
      group = "CLUSTER",
      dependsOn = EXECUTION_MODE_CONFIG,
      triggeredByValue = "CLUSTER"
  )
  public boolean clusterSlaveMemory;

  public static final String CLUSTER_LAUNCHER_ENV_CONFIG = "clusterLauncherEnv";
  public static final String CLUSTER_LAUNCHER_ENV_LABEL = "Launcher ENV";
  public static final String CLUSTER_LAUNCHER_ENV_DESCRIPTION =
      "Sets additional environment variables for the cluster launcher";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = CLUSTER_LAUNCHER_ENV_LABEL,
      description = CLUSTER_LAUNCHER_ENV_DESCRIPTION,
      displayPosition = 20,
      group = "CLUSTER",
      dependsOn = EXECUTION_MODE_CONFIG,
      triggeredByValue = "CLUSTER"
  )
  public Map clusterLauncherEnv;

}
