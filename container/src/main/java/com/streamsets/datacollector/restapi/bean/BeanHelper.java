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
package com.streamsets.datacollector.restapi.bean;

import com.streamsets.datacollector.config.DriftRuleDefinition;
import com.streamsets.datacollector.config.ModelType;
import com.streamsets.datacollector.el.ElConstantDefinition;
import com.streamsets.datacollector.el.ElFunctionArgumentDefinition;
import com.streamsets.datacollector.el.ElFunctionDefinition;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.PreviewOutput;
import com.streamsets.datacollector.execution.alerts.AlertInfo;
import com.streamsets.datacollector.execution.runner.common.SampledRecord;
import com.streamsets.datacollector.record.HeaderImpl;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.runner.production.SourceOffset;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ExecutionMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BeanHelper {
  private BeanHelper() {}

  public static com.streamsets.datacollector.restapi.bean.PipelineStateJson wrapPipelineState(com.streamsets.datacollector.execution.PipelineState pipelineState) {
    if(pipelineState == null) {
      return null;
    }
    return new com.streamsets.datacollector.restapi.bean.PipelineStateJson(pipelineState);
  }

  public static List<com.streamsets.datacollector.restapi.bean.PipelineStateJson> wrapPipelineStatesNewAPI(
    List<com.streamsets.datacollector.execution.PipelineState> pipelineStates) {
    if(pipelineStates == null) {
      return null;
    }
    List<com.streamsets.datacollector.restapi.bean.PipelineStateJson> states = new ArrayList<>(pipelineStates.size());
    for(com.streamsets.datacollector.execution.PipelineState p : pipelineStates) {
      states.add(BeanHelper.wrapPipelineState(p));
    }
    return states;
  }

  public static List<com.streamsets.datacollector.execution.PipelineState> unwrapPipelineStatesNewAPI(
    List<com.streamsets.datacollector.restapi.bean.PipelineStateJson> pipelineStateJsons) {
    if(pipelineStateJsons == null) {
      return null;
    }
    List<com.streamsets.datacollector.execution.PipelineState> states = new ArrayList<>(pipelineStateJsons.size());
    for(com.streamsets.datacollector.restapi.bean.PipelineStateJson p : pipelineStateJsons) {
      states.add(p.getPipelineState());
    }
    return states;
  }

  public static List<PipelineInfoJson> wrapPipelineInfo(List<com.streamsets.datacollector.store.PipelineInfo> pipelines) {
    if(pipelines == null) {
      return null;
    }
    List<PipelineInfoJson> pipelineInfoJson = new ArrayList<>(pipelines.size());
    for(com.streamsets.datacollector.store.PipelineInfo p : pipelines) {
      pipelineInfoJson.add(new PipelineInfoJson(p));
    }
    return pipelineInfoJson;
  }

  public static List<com.streamsets.datacollector.restapi.bean.SnapshotInfoJson> wrapSnapshotInfoNewAPI(
    List<com.streamsets.datacollector.execution.SnapshotInfo> snapshotInfoList) {
    if(snapshotInfoList == null) {
      return null;
    }
    List<com.streamsets.datacollector.restapi.bean.SnapshotInfoJson> snapshotInfoJsonList =
      new ArrayList<>(snapshotInfoList.size());
    for(com.streamsets.datacollector.execution.SnapshotInfo p : snapshotInfoList) {
      snapshotInfoJsonList.add(new com.streamsets.datacollector.restapi.bean.SnapshotInfoJson(p));
    }
    return snapshotInfoJsonList;
  }

  public static com.streamsets.datacollector.restapi.bean.SnapshotInfoJson wrapSnapshotInfoNewAPI(
    com.streamsets.datacollector.execution.SnapshotInfo snapshotInfo) {
    if(snapshotInfo == null) {
      return null;
    }
    return new com.streamsets.datacollector.restapi.bean.SnapshotInfoJson(snapshotInfo);
  }

  public static ConfigConfigurationJson wrapConfigConfiguration(
    Config config) {
    if(config == null) {
      return null;
    }
    return new ConfigConfigurationJson(config);
  }

  public static List<ConfigConfigurationJson> wrapConfigConfiguration(
    List<Config> config) {
    if(config == null) {
      return null;
    }
    List<ConfigConfigurationJson> unwrappedConfig = new ArrayList<>(config.size());
    for(Config c : config) {
      unwrappedConfig.add(new ConfigConfigurationJson(c));
    }
    return unwrappedConfig;
  }

  public static List<Config> unwrapConfigConfiguration(
    List<ConfigConfigurationJson> configConfigurationJson) {
    if(configConfigurationJson == null) {
      return null;
    }
    List<Config> unwrappedConfig = new ArrayList<>(configConfigurationJson.size());
    for(ConfigConfigurationJson c : configConfigurationJson) {
      unwrappedConfig.add(c.getConfigConfiguration());
    }
    return unwrappedConfig;
  }

  public static List<com.streamsets.datacollector.config.StageConfiguration> unwrapStageConfigurations(
    List<StageConfigurationJson> stageConfigurationJson) {
    if(stageConfigurationJson == null) {
      return null;
    }
    List<com.streamsets.datacollector.config.StageConfiguration> configs = new ArrayList<>(stageConfigurationJson.size());
    for(StageConfigurationJson s : stageConfigurationJson) {
      configs.add(s.getStageConfiguration());
    }
    return configs;
  }

  public static com.streamsets.datacollector.config.StageConfiguration unwrapStageConfiguration(
    StageConfigurationJson stageConfigurationJson) {
    if(stageConfigurationJson == null) {
      return null;
    }
    return stageConfigurationJson.getStageConfiguration();
  }

  public static List<StageConfigurationJson> wrapStageConfigurations(
    List<com.streamsets.datacollector.config.StageConfiguration> stageConfiguration) {
    if(stageConfiguration == null) {
      return null;
    }
    List<StageConfigurationJson> configs = new ArrayList<>(stageConfiguration.size());
    for(com.streamsets.datacollector.config.StageConfiguration s : stageConfiguration) {
      configs.add(new StageConfigurationJson(s));
    }
    return configs;
  }

  public static StageConfigurationJson wrapStageConfiguration(
    com.streamsets.datacollector.config.StageConfiguration stageConfiguration) {
    if(stageConfiguration == null) {
      return null;
    }
    return new StageConfigurationJson(stageConfiguration);
  }

  public static List<MetricsRuleDefinitionJson> wrapMetricRuleDefinitions(
    List<com.streamsets.datacollector.config.MetricsRuleDefinition> metricsRuleDefinitions) {
    if(metricsRuleDefinitions == null) {
      return null;
    }
    List<MetricsRuleDefinitionJson> metricsRuleDefinitionJsonList = new ArrayList<>(metricsRuleDefinitions.size());
    for(com.streamsets.datacollector.config.MetricsRuleDefinition m : metricsRuleDefinitions) {
      metricsRuleDefinitionJsonList.add(new MetricsRuleDefinitionJson(m));
    }
    return metricsRuleDefinitionJsonList;
  }

  public static List<com.streamsets.datacollector.config.MetricsRuleDefinition> unwrapMetricRuleDefinitions(
    List<MetricsRuleDefinitionJson> metricsRuleDefinitionJsons) {
    if(metricsRuleDefinitionJsons == null) {
      return null;
    }
    List<com.streamsets.datacollector.config.MetricsRuleDefinition> metricsRuleDefinitionList = new ArrayList<>(metricsRuleDefinitionJsons.size());
    for(MetricsRuleDefinitionJson m : metricsRuleDefinitionJsons) {
      metricsRuleDefinitionList.add(m.getMetricsRuleDefinition());
    }
    return metricsRuleDefinitionList;
  }

  public static List<DataRuleDefinitionJson> wrapDataRuleDefinitions(
    List<com.streamsets.datacollector.config.DataRuleDefinition> dataRuleDefinitions) {
    if(dataRuleDefinitions == null) {
      return null;
    }
    List<DataRuleDefinitionJson> dataRuleDefinitionJsonList = new ArrayList<>(dataRuleDefinitions.size());
    for(com.streamsets.datacollector.config.DataRuleDefinition d : dataRuleDefinitions) {
      dataRuleDefinitionJsonList.add(new DataRuleDefinitionJson(d));
    }
    return dataRuleDefinitionJsonList;
  }

  public static List<com.streamsets.datacollector.config.DataRuleDefinition> unwrapDataRuleDefinitions(
    List<DataRuleDefinitionJson> dataRuleDefinitionJsons) {
    if(dataRuleDefinitionJsons == null) {
      return null;
    }
    List<com.streamsets.datacollector.config.DataRuleDefinition> dataRuleDefinitionList = new ArrayList<>(dataRuleDefinitionJsons.size());
    for(DataRuleDefinitionJson m : dataRuleDefinitionJsons) {
      dataRuleDefinitionList.add(m.getDataRuleDefinition());
    }
    return dataRuleDefinitionList;
  }

  public static List<DriftRuleDefinitionJson> wrapDriftRuleDefinitions(List<DriftRuleDefinition> rules) {
    if(rules == null) {
      return null;
    }
    List<DriftRuleDefinitionJson> rulesJson = new ArrayList<>(rules.size());
    for(DriftRuleDefinition d : rules) {
      rulesJson.add(new DriftRuleDefinitionJson(d));
    }
    return rulesJson;
  }

  public static List<DriftRuleDefinition> unwrapDriftRuleDefinitions(List<DriftRuleDefinitionJson> rulesJson) {
    if(rulesJson == null) {
      return null;
    }
    List<DriftRuleDefinition> rules = new ArrayList<>(rulesJson.size());
    for(DriftRuleDefinitionJson m : rulesJson) {
      rules.add(m.getDriftRuleDefinition());
    }
    return rules;
  }

  public static PipelineConfigurationJson wrapPipelineConfiguration(
    com.streamsets.datacollector.config.PipelineConfiguration pipelineConfiguration) {
    if(pipelineConfiguration == null) {
      return null;
    }
    return new PipelineConfigurationJson(pipelineConfiguration);
  }

  public static com.streamsets.datacollector.config.PipelineConfiguration unwrapPipelineConfiguration(
    PipelineConfigurationJson pipelineConfigurationJson) {
    if(pipelineConfigurationJson == null) {
      return null;
    }
    return pipelineConfigurationJson.getPipelineConfiguration();
  }

  public static PipelineInfo unwrapPipelineInfo(PipelineInfoJson pipelineInfoJson) {
    if(pipelineInfoJson == null) {
      return null;
    }
    return pipelineInfoJson.getPipelineInfo();
  }

  public static PipelineInfoJson wrapPipelineInfo(com.streamsets.datacollector.store.PipelineInfo pipelineInfo) {
    if(pipelineInfo == null) {
      return null;
    }
    return new PipelineInfoJson(pipelineInfo);
  }

  public static List<PipelineRevInfoJson> wrapPipelineRevInfo(
    List<com.streamsets.datacollector.store.PipelineRevInfo> pipelineRevInfos) {
    if(pipelineRevInfos == null) {
      return null;
    }
    List<PipelineRevInfoJson> pipelineRevInfoJsonList = new ArrayList<>(pipelineRevInfos.size());
    for(com.streamsets.datacollector.store.PipelineRevInfo p : pipelineRevInfos) {
      pipelineRevInfoJsonList.add(new PipelineRevInfoJson(p));
    }
    return pipelineRevInfoJsonList;
  }

  public static RuleDefinitionsJson wrapRuleDefinitions(com.streamsets.datacollector.config.RuleDefinitions ruleDefinitions) {
    if(ruleDefinitions == null) {
      return null;
    }
    return new RuleDefinitionsJson(ruleDefinitions);
  }

  public static com.streamsets.datacollector.config.RuleDefinitions unwrapRuleDefinitions(RuleDefinitionsJson ruleDefinitionsJson) {
    if(ruleDefinitionsJson == null) {
      return null;
    }
    return ruleDefinitionsJson.getRuleDefinitions();
  }

  public static List<ConfigDefinitionJson> wrapConfigDefinitions(
    List<com.streamsets.datacollector.config.ConfigDefinition> configDefinitions) {
    if(configDefinitions == null) {
      return null;
    }
    List<ConfigDefinitionJson> configDefinitionlist = new ArrayList<>(configDefinitions.size());
    for(com.streamsets.datacollector.config.ConfigDefinition c : configDefinitions) {
      configDefinitionlist.add(new ConfigDefinitionJson(c));
    }
    return configDefinitionlist;
  }

  public static ModelDefinitionJson wrapModelDefinition(com.streamsets.datacollector.config.ModelDefinition modelDefinition) {
    if(modelDefinition == null) {
      return null;
    }
    return new ModelDefinitionJson(modelDefinition);
  }

  public static RawSourceDefinitionJson wrapRawSourceDefinition(
    com.streamsets.datacollector.config.RawSourceDefinition rawSourceDefinition) {
    if(rawSourceDefinition == null) {
      return null;
    }
    return new RawSourceDefinitionJson(rawSourceDefinition);
  }

  public static ConfigGroupDefinitionJson wrapConfigGroupDefinition(
    com.streamsets.datacollector.config.ConfigGroupDefinition configGroupDefinition) {
    if(configGroupDefinition == null) {
      return null;
    }
    return new ConfigGroupDefinitionJson(configGroupDefinition);
  }

  public static List<StageDefinitionJson> wrapStageDefinitions(
    List<com.streamsets.datacollector.config.StageDefinition> stageDefinitions) {
    if(stageDefinitions == null) {
      return null;
    }
    List<StageDefinitionJson> stageDefinitionJsonList = new ArrayList<>(stageDefinitions.size());
    for(com.streamsets.datacollector.config.StageDefinition s : stageDefinitions) {
      stageDefinitionJsonList.add(new StageDefinitionJson(s));
    }
    return stageDefinitionJsonList;
  }

  public static List<RuleIssueJson> wrapRuleIssues(List<com.streamsets.datacollector.validation.RuleIssue> ruleIssues) {
    if(ruleIssues == null) {
      return null;
    }
    List<RuleIssueJson> ruleIssueJsonList = new ArrayList<>(ruleIssues.size());
    for(com.streamsets.datacollector.validation.RuleIssue r : ruleIssues) {
      ruleIssueJsonList.add(new RuleIssueJson(r));
    }
    return ruleIssueJsonList;
  }

  public static Map<String, List<IssueJson>> wrapIssuesMap(
      Map<String, List<com.streamsets.datacollector.validation.Issue>> stageIssuesMapList) {
    if(stageIssuesMapList == null) {
      return null;
    }
    Map<String, List<IssueJson>> stageIssuesMap = new HashMap<>();
    for(Map.Entry<String, List<com.streamsets.datacollector.validation.Issue>> e : stageIssuesMapList.entrySet()) {
      stageIssuesMap.put(e.getKey(), wrapIssues(e.getValue()));
    }
    return stageIssuesMap;
  }


  public static Map<String, List<com.streamsets.datacollector.validation.Issue>> unwrapIssuesMap(
      Map<String, List<IssueJson>> Issues) {
    if(Issues == null) {
      return null;
    }
    Map<String, List<com.streamsets.datacollector.validation.Issue>> IssuesMap = new HashMap<>();
    for(Map.Entry<String, List<IssueJson>> e : Issues.entrySet()) {
      IssuesMap.put(e.getKey(), unwrapIssues(e.getValue()));
    }
    return IssuesMap;
  }

  public static List<IssueJson> wrapIssues(List<com.streamsets.datacollector.validation.Issue> issues) {
    if(issues == null) {
      return null;
    }
    List<IssueJson> issueJsonList = new ArrayList<>(issues.size());
    for(com.streamsets.datacollector.validation.Issue r : issues) {
      issueJsonList.add(new IssueJson(r));
    }
    return issueJsonList;
  }

  public static List<com.streamsets.datacollector.validation.Issue> unwrapIssues(List<IssueJson> issueJsons) {
    if(issueJsons == null) {
      return null;
    }
    List<com.streamsets.datacollector.validation.Issue> issueList = new ArrayList<>(issueJsons.size());
    for(IssueJson r : issueJsons) {
      issueList.add(r.getIssue());
    }
    return issueList;
  }

  public static IssuesJson wrapIssues(com.streamsets.datacollector.validation.Issues issues) {
    if(issues == null) {
      return null;
    }
    return new IssuesJson(issues);
  }

  public static com.streamsets.pipeline.api.Field unwrapField(FieldJson fieldJson) {
    if(fieldJson == null) {
      return null;
    }
    return fieldJson.getField();
  }

  public static FieldJson wrapField(com.streamsets.pipeline.api.Field field) {
    if(field == null) {
      return null;
    }
    return new FieldJson(field);
  }

  public static HeaderImpl unwrapHeader(HeaderJson headerJson) {
    if(headerJson == null) {
      return null;
    }
    return headerJson.getHeader();
  }

  public static HeaderJson wrapHeader(HeaderImpl header) {
    if(header == null) {
      return null;
    }
    return new HeaderJson(header);
  }

  public static RecordJson wrapRecord(com.streamsets.pipeline.api.Record sourceRecord) {
    if(sourceRecord == null) {
      return null;
    }
    return new RecordJson((RecordImpl)sourceRecord);
  }

  public static com.streamsets.pipeline.api.Record unwrapRecord(RecordJson sourceRecordJson) {
    if(sourceRecordJson == null) {
      return null;
    }
    return sourceRecordJson.getRecord();
  }

  public static List<RecordJson> wrapRecords(List<com.streamsets.pipeline.api.Record> records) {
    if(records == null) {
      return null;
    }
    List<RecordJson> recordJsonList = new ArrayList<>(records.size());
    for(com.streamsets.pipeline.api.Record r : records) {
      recordJsonList.add(new RecordJson((RecordImpl)r));
    }
    return recordJsonList;
  }

  public static List<com.streamsets.pipeline.api.Record> unwrapRecords(List<RecordJson> recordJsons) {
    if(recordJsons == null) {
      return null;
    }
    List<com.streamsets.pipeline.api.Record> recordList = new ArrayList<>(recordJsons.size());
    for(RecordJson r : recordJsons) {
      recordList.add(r.getRecord());
    }
    return recordList;
  }

  public static Map<String, List<RecordJson>> wrapRecordsMap(
    Map<String, List<com.streamsets.pipeline.api.Record>> recordsMap) {
    if(recordsMap == null) {
      return null;
    }
    Map<String, List<RecordJson>> records = new HashMap<>();
    for(Map.Entry<String, List<com.streamsets.pipeline.api.Record>> e : recordsMap.entrySet()) {
      records.put(e.getKey(), BeanHelper.wrapRecords(e.getValue()));
    }
    return records;
  }

  public static Map<String, List<com.streamsets.pipeline.api.Record>> unwrapRecordsMap(
    Map<String, List<RecordJson>> recordsMap) {
    if(recordsMap == null) {
      return null;
    }
    Map<String, List<com.streamsets.pipeline.api.Record>> records = new HashMap<>();
    for(Map.Entry<String, List<RecordJson>> e : recordsMap.entrySet()) {
      records.put(e.getKey(), BeanHelper.unwrapRecords(e.getValue()));
    }
    return records;
  }

  public static List<ErrorMessageJson> wrapErrorMessages(
    List<com.streamsets.pipeline.api.impl.ErrorMessage> errorMessages) {
    if(errorMessages == null) {
      return null;
    }
    List<ErrorMessageJson> errorMessageJsonList = new ArrayList<>(errorMessages.size());
    for(com.streamsets.pipeline.api.impl.ErrorMessage e : errorMessages) {
      errorMessageJsonList.add(new ErrorMessageJson(e));
    }
    return errorMessageJsonList;
  }

  public static List<com.streamsets.pipeline.api.impl.ErrorMessage> unwrapErrorMessages(
    List<ErrorMessageJson> errorMessageJsons) {
    if(errorMessageJsons == null) {
      return null;
    }
    List<com.streamsets.pipeline.api.impl.ErrorMessage> errorMessageList = new ArrayList<>(errorMessageJsons.size());
    for(ErrorMessageJson e : errorMessageJsons) {
      errorMessageList.add(e.getErrorMessage());
    }
    return errorMessageList;
  }

  public static List<com.streamsets.datacollector.runner.StageOutput> unwrapStageOutput(List<StageOutputJson> stageOutputJsons) {
    if(stageOutputJsons == null) {
      return null;
    }
    List<com.streamsets.datacollector.runner.StageOutput> stageOutputList = new ArrayList<>(stageOutputJsons.size());
    for(StageOutputJson s : stageOutputJsons) {
      stageOutputList.add(s.getStageOutput());
    }
    return stageOutputList;
  }

  public static List<StageOutputJson> wrapStageOutput(List<com.streamsets.datacollector.runner.StageOutput> stageOutputs) {
    if(stageOutputs == null) {
      return null;
    }
    List<StageOutputJson> stageOutputList = new ArrayList<>(stageOutputs.size());
    for(com.streamsets.datacollector.runner.StageOutput s : stageOutputs) {
      stageOutputList.add(new StageOutputJson(s));
    }
    return stageOutputList;
  }

  public static List<List<StageOutputJson>> wrapStageOutputLists(
    List<List<com.streamsets.datacollector.runner.StageOutput>> stageOutputs) {
    if(stageOutputs == null) {
      return null;
    }
    List<List<StageOutputJson>> result = new ArrayList<>();
    for(List<com.streamsets.datacollector.runner.StageOutput> stageOutputList : stageOutputs) {
      List<StageOutputJson> stageOutputJson = new ArrayList<>();
      for(com.streamsets.datacollector.runner.StageOutput s : stageOutputList) {
        stageOutputJson.add(new StageOutputJson(s));
      }
      result.add(stageOutputJson);
    }
    return result;
  }

  public static PreviewPipelineOutputJson wrapPreviewPipelineOutput(
    com.streamsets.datacollector.runner.preview.PreviewPipelineOutput previewPipelineOutput) {
    if(previewPipelineOutput == null) {
      return null;
    }
    return new PreviewPipelineOutputJson(previewPipelineOutput);
  }


  public static PreviewOutputJson wrapPreviewOutput(PreviewOutput previewOutput) {
    if(previewOutput == null) {
      return null;
    }
    return new PreviewOutputJson(previewOutput);
  }

  public static PipelineDefinitionJson wrapPipelineDefinition(
    com.streamsets.datacollector.config.PipelineDefinition pipelineDefinition) {
    if(pipelineDefinition == null) {
      return null;
    }
    return new PipelineDefinitionJson(pipelineDefinition);
  }

  public static SourceOffset unwrapSourceOffset(SourceOffsetJson sourceOffsetJson) {
    if(sourceOffsetJson == null) {
      return null;
    }
    return sourceOffsetJson.getSourceOffset();
  }

  public static SourceOffsetJson wrapSourceOffset(SourceOffset sourceOffset) {
    if(sourceOffset == null) {
      return null;
    }
    return new SourceOffsetJson(sourceOffset);
  }

  public static List<ElFunctionArgumentDefinitionJson> wrapElFunctionArgumentDefinitions(
    List<ElFunctionArgumentDefinition> elFunctionArgumentDefinition) {
    if(elFunctionArgumentDefinition == null) {
      return null;
    }
    List<ElFunctionArgumentDefinitionJson> elFunctionArgumentDefinitionJsons =
      new ArrayList<>(elFunctionArgumentDefinition.size());
    for(ElFunctionArgumentDefinition e : elFunctionArgumentDefinition) {
      elFunctionArgumentDefinitionJsons.add(new ElFunctionArgumentDefinitionJson(e));
    }
    return elFunctionArgumentDefinitionJsons;
  }

  public static List<ElFunctionDefinitionJson> wrapElFunctionDefinitions(
    List<ElFunctionDefinition> elFunctionDefinition) {
    if(elFunctionDefinition == null) {
      return null;
    }
    Map<String, ElFunctionDefinitionJson> elFunctionDefinitionJsons =
      new HashMap<>(elFunctionDefinition.size());
    for(ElFunctionDefinition e : elFunctionDefinition) {
      elFunctionDefinitionJsons.put(e.getName(), new ElFunctionDefinitionJson(e));
    }

    return new ArrayList(elFunctionDefinitionJsons.values());
  }

  public static Map<String, ElFunctionDefinitionJson> wrapElFunctionDefinitionsIdx(
      Map<String, ElFunctionDefinition> idx) {
    Map<String, ElFunctionDefinitionJson> jsonIdx = new HashMap<>();
    for (Map.Entry<String, ElFunctionDefinition> e : idx.entrySet()) {
      jsonIdx.put(e.getKey(), new ElFunctionDefinitionJson(e.getValue()));
    }
    return jsonIdx;
  }

  public static List<ElConstantDefinitionJson> wrapElConstantDefinitions(
    List<ElConstantDefinition> elConstantDefinition) {
    if(elConstantDefinition == null) {
      return null;
    }
    Map<String, ElConstantDefinitionJson> elConstantDefinitionJsons =
      new HashMap<>(elConstantDefinition.size());
    for(ElConstantDefinition e : elConstantDefinition) {
      elConstantDefinitionJsons.put(e.getName(), new ElConstantDefinitionJson(e));
    }
    return new ArrayList<>(elConstantDefinitionJsons.values());
  }

  public static Map<String, ElConstantDefinitionJson> wrapElConstantDefinitionsIdx(
      Map<String, ElConstantDefinition> idx) {
    Map<String, ElConstantDefinitionJson> jsonIdx = new HashMap<>();
    for (Map.Entry<String, ElConstantDefinition> e : idx.entrySet()) {
      jsonIdx.put(e.getKey(), new ElConstantDefinitionJson(e.getValue()));
    }
    return jsonIdx;
  }

  public static Map<String, ElFunctionDefinitionJson> wrapElFunctionDefinitionsMap(
    Map<String, ElFunctionDefinition> elFunctionDefinitionMap) {
    if(elFunctionDefinitionMap == null) {
      return null;
    }
    Map<String, ElFunctionDefinitionJson> elFunctionDefinitionJsonMap = new HashMap<>();
    for(Map.Entry<String, ElFunctionDefinition> e : elFunctionDefinitionMap.entrySet()) {
      elFunctionDefinitionJsonMap.put(e.getKey(), new ElFunctionDefinitionJson(e.getValue()));
    }
    return elFunctionDefinitionJsonMap;
  }

  public static Map<String, ElConstantDefinitionJson> wrapElConstantDefinitionsMap(
    Map<String, ElConstantDefinition> elConstantDefinitionMap) {
    if(elConstantDefinitionMap == null) {
      return null;
    }
    Map<String, ElConstantDefinitionJson> elConstantDefinitionJsonMap = new HashMap<>();
    for(Map.Entry<String, ElConstantDefinition> e : elConstantDefinitionMap.entrySet()) {
      elConstantDefinitionJsonMap.put(e.getKey(), new ElConstantDefinitionJson(e.getValue()));
    }
    return elConstantDefinitionJsonMap;
  }

  /*****************************************************/
  /***************** Enum Helper ***********************/
  /*****************************************************/


  public static StatusJson wrapState(com.streamsets.datacollector.execution.PipelineStatus status) {
    if(status == null) {
      return null;
    }
    switch(status) {
      case STOPPED:
        return StatusJson.STOPPED;
      case STOPPING:
        return StatusJson.STOPPING;
      case RUNNING:
        return StatusJson.RUNNING;
      case RUN_ERROR:
        return StatusJson.RUN_ERROR;
      case FINISHED:
        return StatusJson.FINISHED;
      case CONNECTING:
        return StatusJson.CONNECTING;
      case CONNECT_ERROR:
        return StatusJson.CONNECT_ERROR;
      case DISCONNECTED:
        return StatusJson.DISCONNECTED;
      case DISCONNECTING:
        return StatusJson.DISCONNECTING;
      case EDITED:
        return StatusJson.EDITED;
      case FINISHING:
        return StatusJson.FINISHING;
      case KILLED:
        return StatusJson.KILLED;
      case RUNNING_ERROR:
        return StatusJson.RUNNING_ERROR;
      case STARTING:
        return StatusJson.STARTING;
      case START_ERROR:
        return StatusJson.START_ERROR;
      case RETRY:
        return StatusJson.RETRY;
      default:
        throw new IllegalArgumentException("Unrecognized state" + status);

    }
  }

  public static  PipelineStatus unwrapState(StatusJson pipelineStatus) {
    if(pipelineStatus == null) {
      return null;
    }
    switch(pipelineStatus) {
      case STOPPED:
        return com.streamsets.datacollector.execution.PipelineStatus.STOPPED;
      case STOPPING:
        return com.streamsets.datacollector.execution.PipelineStatus.STOPPING;
      case RUNNING:
        return com.streamsets.datacollector.execution.PipelineStatus.RUNNING;
      case RUN_ERROR:
        return com.streamsets.datacollector.execution.PipelineStatus.RUN_ERROR;
      case FINISHED:
        return com.streamsets.datacollector.execution.PipelineStatus.FINISHED;
      case CONNECTING:
        return com.streamsets.datacollector.execution.PipelineStatus.CONNECTING;
      case CONNECT_ERROR:
        return com.streamsets.datacollector.execution.PipelineStatus.CONNECT_ERROR;
      case DISCONNECTED:
        return com.streamsets.datacollector.execution.PipelineStatus.DISCONNECTED;
      case DISCONNECTING:
        return com.streamsets.datacollector.execution.PipelineStatus.DISCONNECTING;
      case EDITED:
        return com.streamsets.datacollector.execution.PipelineStatus.EDITED;
      case FINISHING:
        return com.streamsets.datacollector.execution.PipelineStatus.FINISHING;
      case KILLED:
        return com.streamsets.datacollector.execution.PipelineStatus.KILLED;
      case RUNNING_ERROR:
        return com.streamsets.datacollector.execution.PipelineStatus.RUNNING_ERROR;
      case STARTING:
        return com.streamsets.datacollector.execution.PipelineStatus.STARTING;
      case START_ERROR:
        return com.streamsets.datacollector.execution.PipelineStatus.START_ERROR;
      case RETRY:
        return com.streamsets.datacollector.execution.PipelineStatus.RETRY;
      default:
        throw new IllegalArgumentException("Unrecognized state");
    }
  }

  public static MetricElementJson wrapMetricElement(com.streamsets.datacollector.config.MetricElement metricElement) {
    if(metricElement == null) {
      return null;
    }
    switch(metricElement) {
      //Related to Counters
      case COUNTER_COUNT:
        return MetricElementJson.COUNTER_COUNT;

      //Related to Histogram
      case HISTOGRAM_COUNT:
        return MetricElementJson.HISTOGRAM_COUNT;
      case HISTOGRAM_MAX:
        return MetricElementJson.HISTOGRAM_MAX;
      case HISTOGRAM_MIN:
        return MetricElementJson.HISTOGRAM_MIN;
      case HISTOGRAM_MEAN:
        return MetricElementJson.HISTOGRAM_MEAN;
      case HISTOGRAM_MEDIAN:
        return MetricElementJson.HISTOGRAM_MEDIAN;
      case HISTOGRAM_P75:
        return MetricElementJson.HISTOGRAM_P75;
      case HISTOGRAM_P95:
        return MetricElementJson.HISTOGRAM_P95;
      case HISTOGRAM_P98:
        return MetricElementJson.HISTOGRAM_P98;
      case HISTOGRAM_P99:
        return MetricElementJson.HISTOGRAM_P99;
      case HISTOGRAM_P999:
        return MetricElementJson.HISTOGRAM_P999;
      case HISTOGRAM_STD_DEV:
        return MetricElementJson.HISTOGRAM_STD_DEV;

      //Meters
      case METER_COUNT:
        return MetricElementJson.METER_COUNT;
      case METER_M1_RATE:
        return MetricElementJson.METER_M1_RATE;
      case METER_M5_RATE:
        return MetricElementJson.METER_M5_RATE;
      case METER_M15_RATE:
        return MetricElementJson.METER_M15_RATE;
      case METER_M30_RATE:
        return MetricElementJson.METER_M30_RATE;
      case METER_H1_RATE:
        return MetricElementJson.METER_H1_RATE;
      case METER_H6_RATE:
        return MetricElementJson.METER_H6_RATE;
      case METER_H12_RATE:
        return MetricElementJson.METER_H12_RATE;
      case METER_H24_RATE:
        return MetricElementJson.METER_H24_RATE;
      case METER_MEAN_RATE:
        return MetricElementJson.METER_MEAN_RATE;

      //Timer
      case TIMER_COUNT:
        return MetricElementJson.TIMER_COUNT;
      case TIMER_MAX:
        return MetricElementJson.TIMER_MAX;
      case TIMER_MIN:
        return MetricElementJson.TIMER_MIN;
      case TIMER_MEAN:
        return MetricElementJson.TIMER_MEAN;
      case TIMER_P50:
        return MetricElementJson.TIMER_P50;
      case TIMER_P75:
        return MetricElementJson.TIMER_P75;
      case TIMER_P95:
        return MetricElementJson.TIMER_P95;
      case TIMER_P98:
        return MetricElementJson.TIMER_P98;
      case TIMER_P99:
        return MetricElementJson.TIMER_P99;
      case TIMER_P999:
        return MetricElementJson.TIMER_P999;
      case TIMER_STD_DEV:
        return MetricElementJson.TIMER_STD_DEV;
      case TIMER_M1_RATE:
        return MetricElementJson.TIMER_M1_RATE;
      case TIMER_M5_RATE:
        return MetricElementJson.TIMER_M5_RATE;
      case TIMER_M15_RATE:
        return MetricElementJson.TIMER_M15_RATE;
      case TIMER_MEAN_RATE:
        return MetricElementJson.TIMER_MEAN_RATE;


      //Gauge
      case CURRENT_BATCH_AGE:
        return MetricElementJson.CURRENT_BATCH_AGE;
      case TIME_IN_CURRENT_STAGE:
        return MetricElementJson.TIME_IN_CURRENT_STAGE;
      case TIME_OF_LAST_RECEIVED_RECORD:
        return MetricElementJson.TIME_OF_LAST_RECEIVED_RECORD;

      default:
        throw new IllegalArgumentException("Unrecognized metric element");
    }
  }

  public static com.streamsets.datacollector.config.MetricElement unwrapMetricElement(MetricElementJson metricElementJson) {
    if(metricElementJson == null) {
      return null;
    }
    switch(metricElementJson) {
      //Related to Counters
      case COUNTER_COUNT:
        return com.streamsets.datacollector.config.MetricElement.COUNTER_COUNT;

      //Related to Histogram
      case HISTOGRAM_COUNT:
        return com.streamsets.datacollector.config.MetricElement.HISTOGRAM_COUNT;
      case HISTOGRAM_MAX:
        return com.streamsets.datacollector.config.MetricElement.HISTOGRAM_MAX;
      case HISTOGRAM_MIN:
        return com.streamsets.datacollector.config.MetricElement.HISTOGRAM_MIN;
      case HISTOGRAM_MEAN:
        return com.streamsets.datacollector.config.MetricElement.HISTOGRAM_MEAN;
      case HISTOGRAM_MEDIAN:
        return com.streamsets.datacollector.config.MetricElement.HISTOGRAM_MEDIAN;
      case HISTOGRAM_P75:
        return com.streamsets.datacollector.config.MetricElement.HISTOGRAM_P75;
      case HISTOGRAM_P95:
        return com.streamsets.datacollector.config.MetricElement.HISTOGRAM_P95;
      case HISTOGRAM_P98:
        return com.streamsets.datacollector.config.MetricElement.HISTOGRAM_P98;
      case HISTOGRAM_P99:
        return com.streamsets.datacollector.config.MetricElement.HISTOGRAM_P99;
      case HISTOGRAM_P999:
        return com.streamsets.datacollector.config.MetricElement.HISTOGRAM_P999;
      case HISTOGRAM_STD_DEV:
        return com.streamsets.datacollector.config.MetricElement.HISTOGRAM_STD_DEV;

      //Meters
      case METER_COUNT:
        return com.streamsets.datacollector.config.MetricElement.METER_COUNT;
      case METER_M1_RATE:
        return com.streamsets.datacollector.config.MetricElement.METER_M1_RATE;
      case METER_M5_RATE:
        return com.streamsets.datacollector.config.MetricElement.METER_M5_RATE;
      case METER_M15_RATE:
        return com.streamsets.datacollector.config.MetricElement.METER_M15_RATE;
      case METER_M30_RATE:
        return com.streamsets.datacollector.config.MetricElement.METER_M30_RATE;
      case METER_H1_RATE:
        return com.streamsets.datacollector.config.MetricElement.METER_H1_RATE;
      case METER_H6_RATE:
        return com.streamsets.datacollector.config.MetricElement.METER_H6_RATE;
      case METER_H12_RATE:
        return com.streamsets.datacollector.config.MetricElement.METER_H12_RATE;
      case METER_H24_RATE:
        return com.streamsets.datacollector.config.MetricElement.METER_H24_RATE;
      case METER_MEAN_RATE:
        return com.streamsets.datacollector.config.MetricElement.METER_MEAN_RATE;

      //Timer
      case TIMER_COUNT:
        return com.streamsets.datacollector.config.MetricElement.TIMER_COUNT;
      case TIMER_MAX:
        return com.streamsets.datacollector.config.MetricElement.TIMER_MAX;
      case TIMER_MIN:
        return com.streamsets.datacollector.config.MetricElement.TIMER_MIN;
      case TIMER_MEAN:
        return com.streamsets.datacollector.config.MetricElement.TIMER_MEAN;
      case TIMER_P50:
        return com.streamsets.datacollector.config.MetricElement.TIMER_P50;
      case TIMER_P75:
        return com.streamsets.datacollector.config.MetricElement.TIMER_P75;
      case TIMER_P95:
        return com.streamsets.datacollector.config.MetricElement.TIMER_P95;
      case TIMER_P98:
        return com.streamsets.datacollector.config.MetricElement.TIMER_P98;
      case TIMER_P99:
        return com.streamsets.datacollector.config.MetricElement.TIMER_P99;
      case TIMER_P999:
        return com.streamsets.datacollector.config.MetricElement.TIMER_P999;
      case TIMER_STD_DEV:
        return com.streamsets.datacollector.config.MetricElement.TIMER_STD_DEV;
      case TIMER_M1_RATE:
        return com.streamsets.datacollector.config.MetricElement.TIMER_M1_RATE;
      case TIMER_M5_RATE:
        return com.streamsets.datacollector.config.MetricElement.TIMER_M5_RATE;
      case TIMER_M15_RATE:
        return com.streamsets.datacollector.config.MetricElement.TIMER_M15_RATE;
      case TIMER_MEAN_RATE:
        return com.streamsets.datacollector.config.MetricElement.TIMER_MEAN_RATE;

      //Gauge
      case CURRENT_BATCH_AGE:
        return com.streamsets.datacollector.config.MetricElement.CURRENT_BATCH_AGE;
      case TIME_IN_CURRENT_STAGE:
        return com.streamsets.datacollector.config.MetricElement.TIME_IN_CURRENT_STAGE;
      case TIME_OF_LAST_RECEIVED_RECORD:
        return com.streamsets.datacollector.config.MetricElement.TIME_OF_LAST_RECEIVED_RECORD;

      default:
        throw new IllegalArgumentException("Unrecognized metric element");
    }
  }

  public static MetricTypeJson wrapMetricType(com.streamsets.datacollector.config.MetricType metricType) {
    if(metricType == null) {
      return null;
    }
    switch(metricType) {
      case GAUGE:
        return MetricTypeJson.GAUGE;
      case HISTOGRAM:
        return MetricTypeJson.HISTOGRAM;
      case TIMER:
        return MetricTypeJson.TIMER;
      case COUNTER:
        return MetricTypeJson.COUNTER;
      case METER:
        return MetricTypeJson.METER;
      default:
        throw new IllegalArgumentException("Unrecognized metric type");
    }
  }

  public static com.streamsets.datacollector.config.MetricType unwrapMetricType(MetricTypeJson metricType) {
    if(metricType == null) {
      return null;
    }
    switch(metricType) {
      case GAUGE:
        return com.streamsets.datacollector.config.MetricType.GAUGE;
      case HISTOGRAM:
        return com.streamsets.datacollector.config.MetricType.HISTOGRAM;
      case TIMER:
        return com.streamsets.datacollector.config.MetricType.TIMER;
      case COUNTER:
        return com.streamsets.datacollector.config.MetricType.COUNTER;
      case METER:
        return com.streamsets.datacollector.config.MetricType.METER;
      default:
        throw new IllegalArgumentException("Unrecognized metric type");
    }
  }

  public static ThresholdTypeJson wrapThresholdType(com.streamsets.datacollector.config.ThresholdType thresholdType) {
    if(thresholdType == null) {
      return null;
    }
    switch (thresholdType) {
      case COUNT:
        return ThresholdTypeJson.COUNT;
      case PERCENTAGE:
        return ThresholdTypeJson.PERCENTAGE;
      default:
        throw new IllegalArgumentException("Unrecognized metric type");
    }
  }

  public static com.streamsets.datacollector.config.ThresholdType unwrapThresholdType(ThresholdTypeJson thresholdTypeJson) {
    if(thresholdTypeJson == null) {
      return null;
    }
    switch (thresholdTypeJson) {
      case COUNT:
        return com.streamsets.datacollector.config.ThresholdType.COUNT;
      case PERCENTAGE:
        return com.streamsets.datacollector.config.ThresholdType.PERCENTAGE;
      default:
        throw new IllegalArgumentException("Unrecognized metric type");
    }
  }

  public static ModelTypeJson wrapModelType(com.streamsets.datacollector.config.ModelType modelType) {
    if(modelType == null) {
      return null;
    }
    switch (modelType) {
      case LIST_BEAN:
        return ModelTypeJson.LIST_BEAN;
      case FIELD_SELECTOR_MULTI_VALUE:
        return ModelTypeJson.FIELD_SELECTOR_MULTI_VALUE;
      case FIELD_SELECTOR:
        return ModelTypeJson.FIELD_SELECTOR;
      case PREDICATE:
        return ModelTypeJson.PREDICATE;
      case VALUE_CHOOSER:
        return ModelTypeJson.VALUE_CHOOSER;
      case MULTI_VALUE_CHOOSER:
        return ModelTypeJson.MULTI_VALUE_CHOOSER;
      default:
        throw new IllegalArgumentException("Unrecognized model type");
    }
  }

  public static com.streamsets.datacollector.config.ModelType unwrapModelType(ModelTypeJson modelTypeJson) {
    if(modelTypeJson == null) {
      return null;
    }
    switch (modelTypeJson) {
      case LIST_BEAN:
        return ModelType.LIST_BEAN;
      case FIELD_SELECTOR_MULTI_VALUE:
        return ModelType.FIELD_SELECTOR_MULTI_VALUE;
      case FIELD_SELECTOR:
        return ModelType.FIELD_SELECTOR;
      case PREDICATE:
        return ModelType.PREDICATE;
      case VALUE_CHOOSER:
        return ModelType.VALUE_CHOOSER;
      case MULTI_VALUE_CHOOSER:
        return ModelType.MULTI_VALUE_CHOOSER;
      default:
        throw new IllegalArgumentException("Unrecognized model type");
    }
  }

  public static StageTypeJson wrapStageType(com.streamsets.datacollector.config.StageType stageType) {
    if(stageType == null) {
      return null;
    }
    switch (stageType) {
      case TARGET:
        return StageTypeJson.TARGET;
      case SOURCE:
        return StageTypeJson.SOURCE;
      case PROCESSOR:
        return StageTypeJson.PROCESSOR;
      default:
        throw new IllegalArgumentException("Unrecognized model type");
    }
  }

  public static com.streamsets.datacollector.config.StageType unwrapStageType(StageTypeJson stageTypeJson) {
    if(stageTypeJson == null) {
      return null;
    }
    switch (stageTypeJson) {
      case TARGET:
        return com.streamsets.datacollector.config.StageType.TARGET;
      case SOURCE:
        return com.streamsets.datacollector.config.StageType.SOURCE;
      case PROCESSOR:
        return com.streamsets.datacollector.config.StageType.PROCESSOR;
      default:
        throw new IllegalArgumentException("Unrecognized model type");
    }
  }

  public static com.streamsets.datacollector.restapi.bean.CallbackInfoJson wrapCallbackInfo(com.streamsets.datacollector.callback.CallbackInfo callbackInfo) {
    if(callbackInfo == null) {
      return null;
    }
    return new com.streamsets.datacollector.restapi.bean.CallbackInfoJson(callbackInfo);
  }

  public static ExecutionModeJson wrapExecutionMode(ExecutionMode executionMode) {
    if (executionMode == null) {
      return null;
    }
    switch (executionMode) {
      case CLUSTER_BATCH:
        return ExecutionModeJson.CLUSTER_BATCH;
      case CLUSTER_YARN_STREAMING:
        return ExecutionModeJson.CLUSTER_YARN_STREAMING;
      case CLUSTER_MESOS_STREAMING:
        return ExecutionModeJson.CLUSTER_MESOS_STREAMING;
      case STANDALONE:
        return ExecutionModeJson.STANDALONE;
      case SLAVE:
        return ExecutionModeJson.SLAVE;
      default:
        throw new IllegalArgumentException("Unrecognized execution mode: " + executionMode);
    }
  }

  public static ExecutionMode unwrapExecutionMode(ExecutionModeJson executionModeJson) {
    if (executionModeJson == null) {
      return null;
    }
    switch (executionModeJson) {
      case CLUSTER:
        return ExecutionMode.CLUSTER;
      case CLUSTER_BATCH:
        return ExecutionMode.CLUSTER_BATCH;
      case CLUSTER_YARN_STREAMING:
        return ExecutionMode.CLUSTER_YARN_STREAMING;
      case CLUSTER_MESOS_STREAMING:
        return ExecutionMode.CLUSTER_MESOS_STREAMING;
      case STANDALONE:
        return ExecutionMode.STANDALONE;
      case SLAVE:
        return ExecutionMode.SLAVE;
      default:
        throw new IllegalArgumentException("Unrecognized execution mode: " + executionModeJson);
    }
  }

  public static List<AlertInfoJson> wrapAlertInfoList(List<AlertInfo> alertInfoList) {
    if(alertInfoList == null) {
      return null;
    }

    List<AlertInfoJson> alertInfoJsonList = new ArrayList<>();

    for(AlertInfo alertInfo: alertInfoList) {
      alertInfoJsonList.add(new AlertInfoJson(alertInfo));
    }

    return alertInfoJsonList;
  }

  public static List<SampledRecordJson> wrapSampledRecords(List<SampledRecord> sampledRecords) {
    if(sampledRecords == null) {
      return null;
    }
    List<SampledRecordJson> recordJsonList = new ArrayList<>(sampledRecords.size());
    for(SampledRecord sampledRecord : sampledRecords) {
      recordJsonList.add(new SampledRecordJson(sampledRecord));
    }
    return recordJsonList;
  }
}
