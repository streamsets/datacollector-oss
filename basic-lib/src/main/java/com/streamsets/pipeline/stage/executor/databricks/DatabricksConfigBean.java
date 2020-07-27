/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.executor.databricks;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.VaultEL;
import com.streamsets.pipeline.lib.http.HttpProxyConfigBean;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatabricksConfigBean {

  @ConfigDefBean(groups = "CREDENTIALS")
  public CredentialsConfigBean credentialsConfigBean = new CredentialsConfigBean();

  @ConfigDefBean(groups = "PROXY")
  public HttpProxyConfigBean proxyConfigBean = new HttpProxyConfigBean();

  @ConfigDefBean(groups = "TLS")
  public TlsConfigBean tlsConfigBean = new TlsConfigBean();

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      required = true,
      label = "Cluster Base URL",
      group = "APPLICATION",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String baseUrl = "";

  @ConfigDef(
      type = ConfigDef.Type.BOOLEAN,
      required = true,
      label = "Use Proxy",
      defaultValue = "false",
      group = "APPLICATION",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED
  )
  public boolean useProxy = false;
  /*
   * Application Group
   */

  @ConfigDef(
      type = ConfigDef.Type.MODEL,
      required = true,
      label = "Job Type",
      group = "APPLICATION",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @ValueChooserModel(JobTypeChooserValues.class)
  public JobType jobType;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      required = true,
      label = "Job ID",
      group = "APPLICATION",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public int jobId;

  @ConfigDef(
      type = ConfigDef.Type.LIST,
      required = false,
      label = "Parameters",
      group = "APPLICATION",
      dependsOn = "jobType",
      triggeredByValue = "JAR",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      elDefs = {RecordEL.class, VaultEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public List<String> jarParams;

  @ConfigDef(
      type = ConfigDef.Type.MAP,
      required = false,
      label = "Parameters",
      group = "APPLICATION",
      dependsOn = "jobType",
      triggeredByValue = "NOTEBOOK",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      elDefs = {RecordEL.class, VaultEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public Map<String, String> notebookParams;

  private ELEval elEval;
  private ELVars elVars;

  public List<Stage.ConfigIssue> init(Stage.Context context, String prefix) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();

    elVars = context.createELVars();
    if (jobType == JobType.JAR) {
      elEval = context.createELEval("jarParams");

      for (String arg : jarParams) {
        try {
          context.parseEL(arg);
        } catch (ELEvalException ex) { // NOSONAR
          issues.add(context.createConfigIssue(
              "APPLICATION", prefix + "jarParams", ex.getErrorCode(), ex.getParams()));
        }
      }
    } else {
      elEval = context.createELEval("notebookParams");
      for (Map.Entry<String, String> arg : notebookParams.entrySet()) {
        try {
          context.parseEL(arg.getKey());
          context.parseEL(arg.getValue());
        } catch (ELEvalException ex) { // NOSONAR
          issues.add(context.createConfigIssue(
              "APPLICATION", prefix + "notebookParams", ex.getErrorCode(), ex.getParams()));
        }
      }
    }
    tlsConfigBean.init(context, "HTTP", prefix + "tlsConfigBean.", issues);
    return issues;
  }

  List<String> evaluateJarELs(Record record) throws ELEvalException {
    RecordEL.setRecordInContext(elVars, record);
    List<String> evaluatedArgs = new ArrayList<>(jarParams.size());
    for (String arg : jarParams) {
      evaluatedArgs.add(elEval.eval(elVars, arg, String.class));
    }

    return evaluatedArgs;
  }

  Map<String, String> evaluateNotebookELs(Record record) throws ELEvalException {
    RecordEL.setRecordInContext(elVars, record);
    Map<String, String> evaluatedArgs = new HashMap<>(notebookParams.size());
    for (Map.Entry<String, String> arg : notebookParams.entrySet()) {
      evaluatedArgs.put(
          elEval.eval(elVars, arg.getKey(), String.class), elEval.eval(elVars, arg.getValue(), String.class));
    }
    return evaluatedArgs;
  }

}
