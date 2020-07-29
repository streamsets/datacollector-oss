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
package com.streamsets.datacollector.pipeline.executor.spark.yarn;

import com.streamsets.datacollector.pipeline.executor.spark.DeployMode;
import com.streamsets.datacollector.pipeline.executor.spark.DeployModeChooserValues;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.VaultEL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class YarnConfigBean {
  @ConfigDef(
      type = ConfigDef.Type.MODEL,
      required = true,
      label = "Deploy Mode",
      group = "SPARK",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @ValueChooserModel(DeployModeChooserValues.class)
  public DeployMode deployMode;

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      required = true,
      label = "Driver Memory",
      group = "SPARK",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String driverMemory = "";

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      required = true,
      label = "Executor Memory",
      group = "SPARK",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String executorMemory = "";

  @ConfigDef(
      type = ConfigDef.Type.BOOLEAN,
      required = true,
      defaultValue = "true",
      label = "Dynamic Allocation",
      description = "Enable the dynamic allocation of Spark worker nodes",
      group = "SPARK",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public boolean dynamicAllocation = true;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      required = true,
      min = 0,
      label = "Minimum Number of Worker Nodes",
      dependsOn = "dynamicAllocation",
      triggeredByValue = "true",
      group = "SPARK",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public int minExecutors;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      required = true,
      min = 0,
      label = "Maximum Number of Worker Nodes",
      dependsOn = "dynamicAllocation",
      triggeredByValue = "true",
      group = "SPARK",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public int maxExecutors;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      required = true,
      min = 1,
      label = "Number of Worker Nodes",
      dependsOn = "dynamicAllocation",
      triggeredByValue = "false",
      group = "SPARK",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public int numExecutors;

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      required = false,
      label = "Proxy User",
      group = "SPARK",
      displayPosition = 110,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String proxyUser = "";

  @ConfigDef(
      type = ConfigDef.Type.LIST,
      required = false,
      label = "Additional Spark Arguments",
      description = "Use this to pass any additional arguments to Spark Launcher/Spark Submit. Overrides other parameters",
      group = "SPARK",
      displayPosition = 140,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public List<String> noValueArgs = new ArrayList<>();

  @ConfigDef(
      type = ConfigDef.Type.MAP,
      required = false,
      label = "Additional Spark Arguments and Values",
      description = "Use this to pass any additional arguments to Spark Launcher/Spark Submit. Overrides other parameters",
      group = "SPARK",
      displayPosition = 150,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public Map<String, String> args = new HashMap<>();

  @ConfigDef(
      type = ConfigDef.Type.MAP,
      required = false,
      label = "Environment Variables",
      group = "SPARK",
      displayPosition = 160,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public Map<String, String> env = new HashMap<>();

  /*
   * APPLICATION group.
   */
  @ConfigDef(
      type = ConfigDef.Type.MODEL,
      required = true,
      label = "Language",
      group = "APPLICATION",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @ValueChooserModel(LanguageChooserValues.class)
  public Language language;

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      required = true,
      label = "Application Name",
      group = "APPLICATION",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String appName = "";

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      required = true,
      label = "Application Resource",
      group = "APPLICATION",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String appResource = "";

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      required = true,
      label = "Main Class",
      group = "APPLICATION",
      dependsOn = "language",
      triggeredByValue = "JVM",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String mainClass = "";

  @ConfigDef(
      type = ConfigDef.Type.LIST,
      required = false,
      label = "Application Arguments",
      elDefs = {RecordEL.class, VaultEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      group = "APPLICATION",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public List<String> appArgs = new ArrayList<>();

  @ConfigDef(
      type = ConfigDef.Type.LIST,
      required = false,
      label = "Additional JARs",
      group = "APPLICATION",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public List<String> additionalJars = new ArrayList<>();

  @ConfigDef(
      type = ConfigDef.Type.LIST,
      required = false,
      label = "Dependencies",
      description = "Full path to additional Python files required by the application resource",
      group = "APPLICATION",
      dependsOn = "language",
      triggeredByValue = "PYTHON",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public List<String> pyFiles = new ArrayList<>();

  @ConfigDef(
      type = ConfigDef.Type.LIST,
      required = false,
      label = "Additional Files",
      group = "APPLICATION",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.ADVANCED
  )
  public List<String> additionalFiles = new ArrayList<>();

  @ConfigDef(
      type = ConfigDef.Type.BOOLEAN,
      required = true,
      defaultValue = "false",
      label = "Wait for Completion",
      dependsOn = "deployMode",
      triggeredByValue = "CLUSTER",
      group = "APPLICATION",
      displayPosition = 90,
      displayMode = ConfigDef.DisplayMode.ADVANCED
  )
  public boolean waitForCompletion;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      required = true,
      defaultValue = "0",
      min = 0,
      label = "Maximum Time to Wait (ms)",
      description = "Time to wait for the app to complete. 0 to wait forever.",
      elDefs = TimeEL.class,
      dependsOn = "waitForCompletion",
      triggeredByValue = "true",
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.ADVANCED
  )
  public long waitTimeout;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      required = true,
      defaultValue = "0",
      min = 0,
      max = 20,
      label = "Spark App Submission Time (s)",
      description = "Time to wait for the Spark app to be submitted successfully. Note that this cause batch delay" +
          "Enter 0 to not wait for app submission. May cause appId to not be updated",
      group = "APPLICATION",
      displayPosition = 110,
      displayMode = ConfigDef.DisplayMode.ADVANCED
  )
  public long submitTimeout;

  @ConfigDef(
      type = ConfigDef.Type.BOOLEAN,
      required = true,
      defaultValue = "false",
      label = "Enable Verbose Logging",
      description = "Enable only for testing, as a lot of additional log data is written to sdc.log",
      group = "APPLICATION",
      displayPosition = 120,
      displayMode = ConfigDef.DisplayMode.ADVANCED
  )
  public boolean verbose;

  private ELEval elEval;
  private ELVars elVars;

  public List<Stage.ConfigIssue> init(Stage.Context context, String prefix) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();

    elVars = context.createELVars();
    elEval = context.createELEval("appArgs");

    for (String arg : appArgs) {
      try {
        context.parseEL(arg);
      } catch (ELEvalException ex) { // NOSONAR
        issues.add(context.createConfigIssue(
            "APPLICATION", prefix + "appArgs", ex.getErrorCode(), ex.getParams()));
      }
    }
    return issues;
  }

  List<String> evaluateArgsELs(Record record) throws ELEvalException {
    RecordEL.setRecordInContext(elVars, record);
    List<String> evaluatedArgs = new ArrayList<>(appArgs.size());
    for (String arg : appArgs) {
      evaluatedArgs.add(elEval.eval(elVars, arg, String.class));
    }
    return evaluatedArgs;
  }
}
