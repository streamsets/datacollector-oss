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
package com.streamsets.datacollector.restapi.bean;

import com.streamsets.datacollector.bundles.BundleContentGeneratorDefinition;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.ConfigGroupDefinition;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.DriftRuleDefinition;
import com.streamsets.datacollector.config.MetricElement;
import com.streamsets.datacollector.config.MetricType;
import com.streamsets.datacollector.config.MetricsRuleDefinition;
import com.streamsets.datacollector.config.ModelDefinition;
import com.streamsets.datacollector.config.ModelType;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.PipelineDefinition;
import com.streamsets.datacollector.config.PipelineFragmentConfiguration;
import com.streamsets.datacollector.config.PipelineFragmentDefinition;
import com.streamsets.datacollector.config.PipelineRulesDefinition;
import com.streamsets.datacollector.config.RawSourceDefinition;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.config.ServiceConfiguration;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.ServiceDependencyDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.ThresholdType;
import com.streamsets.datacollector.el.ElConstantDefinition;
import com.streamsets.datacollector.el.ElFunctionArgumentDefinition;
import com.streamsets.datacollector.el.ElFunctionDefinition;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.PreviewOutput;
import com.streamsets.datacollector.execution.SnapshotInfo;
import com.streamsets.datacollector.execution.alerts.AlertInfo;
import com.streamsets.datacollector.execution.runner.common.SampledRecord;
import com.streamsets.datacollector.record.HeaderImpl;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.runner.production.SourceOffset;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineRevInfo;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.pipeline.api.AntennaDoctorMessage;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.HideStage;
import com.streamsets.pipeline.api.StageType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BeanHelper {
  private BeanHelper() {}

  public static PipelineStateJson wrapPipelineState(PipelineState pipelineState) {
    if(pipelineState == null) {
      return null;
    }
    return new PipelineStateJson(pipelineState, false);
  }

  public static PipelineStateJson wrapPipelineState(PipelineState pipelineState, boolean ignoreMetrics) {
    if(pipelineState == null) {
      return null;
    }
    return new PipelineStateJson(pipelineState, ignoreMetrics);
  }

  public static List<PipelineStateJson> wrapPipelineStatesNewAPI(
      List<PipelineState> pipelineStates,
      boolean ignoreMetrics
  ) {
    if(pipelineStates == null) {
      return null;
    }
    List<PipelineStateJson> states = new ArrayList<>(pipelineStates.size());
    for(PipelineState p : pipelineStates) {
      states.add(BeanHelper.wrapPipelineState(p, ignoreMetrics));
    }
    return states;
  }

  public static List<PipelineState> unwrapPipelineStatesNewAPI(List<PipelineStateJson> pipelineStateJsons) {
    if(pipelineStateJsons == null) {
      return null;
    }
    List<PipelineState> states = new ArrayList<>(pipelineStateJsons.size());
    for(PipelineStateJson p : pipelineStateJsons) {
      states.add(p.getPipelineState());
    }
    return states;
  }

  public static List<PipelineInfoJson> wrapPipelineInfo(List<PipelineInfo> pipelines) {
    if(pipelines == null) {
      return null;
    }
    List<PipelineInfoJson> pipelineInfoJson = new ArrayList<>(pipelines.size());
    for(com.streamsets.datacollector.store.PipelineInfo p : pipelines) {
      pipelineInfoJson.add(new PipelineInfoJson(p));
    }
    return pipelineInfoJson;
  }

  public static List<SnapshotInfoJson> wrapSnapshotInfoNewAPI(List<SnapshotInfo> snapshotInfoList) {
    if(snapshotInfoList == null) {
      return null;
    }
    List<SnapshotInfoJson> snapshotInfoJsonList =
      new ArrayList<>(snapshotInfoList.size());
    for(SnapshotInfo p : snapshotInfoList) {
      snapshotInfoJsonList.add(new SnapshotInfoJson(p));
    }
    return snapshotInfoJsonList;
  }

  public static SnapshotInfoJson wrapSnapshotInfoNewAPI(SnapshotInfo snapshotInfo) {
    if(snapshotInfo == null) {
      return null;
    }
    return new SnapshotInfoJson(snapshotInfo);
  }

  public static ConfigConfigurationJson wrapConfigConfiguration(Config config) {
    if(config == null) {
      return null;
    }
    return new ConfigConfigurationJson(config);
  }

  public static List<ConfigConfigurationJson> wrapConfigConfiguration(List<Config> config) {
    if(config == null) {
      return Collections.emptyList();
    }
    List<ConfigConfigurationJson> unwrappedConfig = new ArrayList<>(config.size());
    for(Config c : config) {
      unwrappedConfig.add(new ConfigConfigurationJson(c));
    }
    return unwrappedConfig;
  }

  public static List<Config> unwrapConfigConfiguration(List<ConfigConfigurationJson> configConfigurationJson) {
    if(configConfigurationJson == null) {
      return null;
    }
    List<Config> unwrappedConfig = new ArrayList<>(configConfigurationJson.size());
    for(ConfigConfigurationJson c : configConfigurationJson) {
      unwrappedConfig.add(c.getConfigConfiguration());
    }
    return unwrappedConfig;
  }

  public static List<StageConfiguration> unwrapStageConfigurations(
    List<StageConfigurationJson> stageConfigurationJson
  ) {
    if(stageConfigurationJson == null) {
      return null;
    }
    List<StageConfiguration> configs = new ArrayList<>(stageConfigurationJson.size());
    for(StageConfigurationJson s : stageConfigurationJson) {
      configs.add(s.getStageConfiguration());
    }
    return configs;
  }

  public static StageConfiguration unwrapStageConfiguration(StageConfigurationJson stageConfigurationJson) {
    if(stageConfigurationJson == null) {
      return null;
    }
    return stageConfigurationJson.getStageConfiguration();
  }

  public static List<PipelineFragmentConfiguration> unwrapPipelineFragementConfigurations(
      List<PipelineFragmentConfigurationJson> fragmentConfigurationJson
  ) {
    if (fragmentConfigurationJson == null) {
      return null;
    }
    List<PipelineFragmentConfiguration> configs = new ArrayList<>(fragmentConfigurationJson.size());
    for (PipelineFragmentConfigurationJson s : fragmentConfigurationJson) {
      configs.add(s.getFragmentConfiguration());
    }
    return configs;
  }

  public static List<PipelineFragmentConfigurationJson> wrapPipelineFragmentConfigurations(
      List<PipelineFragmentConfiguration> fragmentConfigurations
  ) {
    if(fragmentConfigurations == null) {
      return null;
    }
    List<PipelineFragmentConfigurationJson> configs = new ArrayList<>(fragmentConfigurations.size());
    for(PipelineFragmentConfiguration s : fragmentConfigurations) {
      configs.add(new PipelineFragmentConfigurationJson(s));
    }
    return configs;
  }

  public static List<StageConfigurationJson> wrapStageConfigurations(List<StageConfiguration> stageConfiguration) {
    if(stageConfiguration == null) {
      return null;
    }
    List<StageConfigurationJson> configs = new ArrayList<>(stageConfiguration.size());
    for(StageConfiguration s : stageConfiguration) {
      configs.add(new StageConfigurationJson(s));
    }
    return configs;
  }

  public static StageConfigurationJson wrapStageConfiguration(StageConfiguration stageConfiguration) {
    if(stageConfiguration == null) {
      return null;
    }
    return new StageConfigurationJson(stageConfiguration);
  }

  public static List<MetricsRuleDefinitionJson> wrapMetricRuleDefinitions(
      List<MetricsRuleDefinition> metricsRuleDefinitions
  ) {
    if(metricsRuleDefinitions == null) {
      return null;
    }
    List<MetricsRuleDefinitionJson> metricsRuleDefinitionJsonList = new ArrayList<>(metricsRuleDefinitions.size());
    for(MetricsRuleDefinition m : metricsRuleDefinitions) {
      metricsRuleDefinitionJsonList.add(new MetricsRuleDefinitionJson(m));
    }
    return metricsRuleDefinitionJsonList;
  }

  public static List<MetricsRuleDefinition> unwrapMetricRuleDefinitions(
    List<MetricsRuleDefinitionJson> metricsRuleDefinitionJsons
  ) {
    if(metricsRuleDefinitionJsons == null) {
      return null;
    }
    List<MetricsRuleDefinition> metricsRuleDefinitionList = new ArrayList<>(metricsRuleDefinitionJsons.size());
    for(MetricsRuleDefinitionJson m : metricsRuleDefinitionJsons) {
      metricsRuleDefinitionList.add(m.getMetricsRuleDefinition());
    }
    return metricsRuleDefinitionList;
  }

  public static List<DataRuleDefinitionJson> wrapDataRuleDefinitions(
    List<DataRuleDefinition> dataRuleDefinitions
  ) {
    if(dataRuleDefinitions == null) {
      return null;
    }
    List<DataRuleDefinitionJson> dataRuleDefinitionJsonList = new ArrayList<>(dataRuleDefinitions.size());
    for(DataRuleDefinition d : dataRuleDefinitions) {
      dataRuleDefinitionJsonList.add(new DataRuleDefinitionJson(d));
    }
    return dataRuleDefinitionJsonList;
  }

  public static List<DataRuleDefinition> unwrapDataRuleDefinitions(
    List<DataRuleDefinitionJson> dataRuleDefinitionJsons
  ) {
    if(dataRuleDefinitionJsons == null) {
      return null;
    }
    List<DataRuleDefinition> dataRuleDefinitionList = new ArrayList<>(dataRuleDefinitionJsons.size());
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

  public static PipelineConfigurationJson wrapPipelineConfiguration(PipelineConfiguration pipelineConfiguration) {
    if(pipelineConfiguration == null) {
      return null;
    }
    return new PipelineConfigurationJson(pipelineConfiguration);
  }

  public static PipelineConfiguration unwrapPipelineConfiguration(PipelineConfigurationJson pipelineConfigurationJson) {
    if(pipelineConfigurationJson == null) {
      return null;
    }
    return pipelineConfigurationJson.getPipelineConfiguration();
  }

  public static PipelineFragmentConfigurationJson wrapPipelineFragmentConfiguration(
      PipelineFragmentConfiguration pipelineFragmentConfiguration
  ) {
    if(pipelineFragmentConfiguration == null) {
      return null;
    }
    return new PipelineFragmentConfigurationJson(pipelineFragmentConfiguration);
  }

  public static PipelineFragmentConfiguration unwrapPipelineFragmentConfiguration(
      PipelineFragmentConfigurationJson pipelineFragmentConfigurationJson
  ) {
    if(pipelineFragmentConfigurationJson == null) {
      return null;
    }
    return pipelineFragmentConfigurationJson.getFragmentConfiguration();
  }

  public static PipelineInfo unwrapPipelineInfo(PipelineInfoJson pipelineInfoJson) {
    if(pipelineInfoJson == null) {
      return null;
    }
    return pipelineInfoJson.getPipelineInfo();
  }

  public static PipelineInfoJson wrapPipelineInfo(PipelineInfo pipelineInfo) {
    if(pipelineInfo == null) {
      return null;
    }
    return new PipelineInfoJson(pipelineInfo);
  }

  public static List<PipelineRevInfoJson> wrapPipelineRevInfo(List<PipelineRevInfo> pipelineRevInfos) {
    if(pipelineRevInfos == null) {
      return null;
    }
    List<PipelineRevInfoJson> pipelineRevInfoJsonList = new ArrayList<>(pipelineRevInfos.size());
    for(PipelineRevInfo p : pipelineRevInfos) {
      pipelineRevInfoJsonList.add(new PipelineRevInfoJson(p));
    }
    return pipelineRevInfoJsonList;
  }

  public static RuleDefinitionsJson wrapRuleDefinitions(RuleDefinitions ruleDefinitions) {
    if(ruleDefinitions == null) {
      return null;
    }
    return new RuleDefinitionsJson(ruleDefinitions);
  }

  public static RuleDefinitions unwrapRuleDefinitions(RuleDefinitionsJson ruleDefinitionsJson) {
    if(ruleDefinitionsJson == null) {
      return null;
    }
    return ruleDefinitionsJson.getRuleDefinitions();
  }

  public static List<ConfigDefinitionJson> wrapConfigDefinitions(List<ConfigDefinition> configDefinitions) {
    if(configDefinitions == null) {
      return null;
    }
    List<ConfigDefinitionJson> configDefinitionlist = new ArrayList<>(configDefinitions.size());
    for(ConfigDefinition c : configDefinitions) {
      configDefinitionlist.add(new ConfigDefinitionJson(c));
    }
    return configDefinitionlist;
  }

  public static ModelDefinitionJson wrapModelDefinition(ModelDefinition modelDefinition) {
    if(modelDefinition == null) {
      return null;
    }
    return new ModelDefinitionJson(modelDefinition);
  }

  public static RawSourceDefinitionJson wrapRawSourceDefinition(RawSourceDefinition rawSourceDefinition) {
    if(rawSourceDefinition == null) {
      return null;
    }
    return new RawSourceDefinitionJson(rawSourceDefinition);
  }

  public static ConfigGroupDefinitionJson wrapConfigGroupDefinition(ConfigGroupDefinition configGroupDefinition) {
    if(configGroupDefinition == null) {
      return null;
    }
    return new ConfigGroupDefinitionJson(configGroupDefinition);
  }

  public static List<StageDefinitionJson> wrapStageDefinitions(List<StageDefinition> stageDefinitions) {
    if(stageDefinitions == null) {
      return null;
    }
    List<StageDefinitionJson> stageDefinitionJsonList = new ArrayList<>(stageDefinitions.size());
    for(StageDefinition s : stageDefinitions) {
      stageDefinitionJsonList.add(new StageDefinitionJson(s));
    }
    return stageDefinitionJsonList;
  }

  public static List<ServiceDefinitionJson> wrapServiceDefinitions(List<ServiceDefinition> serviceDefinitions) {
    if(serviceDefinitions == null) {
      return null;
    }

    return serviceDefinitions.stream()
      .map(ServiceDefinitionJson::new)
      .collect(Collectors.toCollection(ArrayList::new));
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
      Map<String, List<Issue>> stageIssuesMapList
  ) {
    if(stageIssuesMapList == null) {
      return null;
    }
    Map<String, List<IssueJson>> stageIssuesMap = new HashMap<>();
    for(Map.Entry<String, List<Issue>> e : stageIssuesMapList.entrySet()) {
      stageIssuesMap.put(e.getKey(), wrapIssues(e.getValue()));
    }
    return stageIssuesMap;
  }


  public static Map<String, List<Issue>> unwrapIssuesMap(
      Map<String, List<IssueJson>> Issues
  ) {
    if(Issues == null) {
      return null;
    }
    Map<String, List<Issue>> IssuesMap = new HashMap<>();
    for(Map.Entry<String, List<IssueJson>> e : Issues.entrySet()) {
      IssuesMap.put(e.getKey(), unwrapIssues(e.getValue()));
    }
    return IssuesMap;
  }

  public static List<IssueJson> wrapIssues(List<Issue> issues) {
    if(issues == null) {
      return null;
    }
    List<IssueJson> issueJsonList = new ArrayList<>(issues.size());
    for(Issue r : issues) {
      issueJsonList.add(new IssueJson(r));
    }
    return issueJsonList;
  }

  public static List<Issue> unwrapIssues(List<IssueJson> issueJsons) {
    if(issueJsons == null) {
      return null;
    }
    List<Issue> issueList = new ArrayList<>(issueJsons.size());
    for(IssueJson r : issueJsons) {
      issueList.add(r.getIssue());
    }
    return issueList;
  }

  public static IssuesJson wrapIssues(Issues issues) {
    if(issues == null) {
      return null;
    }
    return new IssuesJson(issues);
  }

  public static Issues unwrapIssues(IssuesJson issuesJson) {
    if(issuesJson == null) {
      return null;
    }
    Issues issues = new Issues();
    issues.addAll(unwrapIssues(issuesJson.getPipelineIssues()));
    for(Map.Entry<String, List<IssueJson>> e : issuesJson.getStageIssues().entrySet()) {
      issues.addAll(unwrapIssues(e.getValue()));
    }
    return issues;
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

  public static PipelineDefinitionJson wrapPipelineDefinition(PipelineDefinition pipelineDefinition) {
    if(pipelineDefinition == null) {
      return null;
    }
    return new PipelineDefinitionJson(pipelineDefinition);
  }

  public static PipelineFragmentDefinitionJson wrapPipelineFragmentDefinition(
      PipelineFragmentDefinition pipelineFragmentDefinition
  ) {
    if(pipelineFragmentDefinition == null) {
      return null;
    }
    return new PipelineFragmentDefinitionJson(pipelineFragmentDefinition);
  }

  public static PipelineRulesDefinitionJson wrapPipelineRulesDefinition(
      PipelineRulesDefinition pipelineRulesDefinition
  ) {
    if(pipelineRulesDefinition == null) {
      return null;
    }
    return new PipelineRulesDefinitionJson(pipelineRulesDefinition);
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

    return new ArrayList<>(elFunctionDefinitionJsons.values());
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


  public static StatusJson wrapState(PipelineStatus status) {
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
      case STARTING_ERROR:
        return StatusJson.STARTING_ERROR;
      case START_ERROR:
        return StatusJson.START_ERROR;
      case RETRY:
        return StatusJson.RETRY;
      case STOP_ERROR:
        return StatusJson.STOP_ERROR;
      case STOPPING_ERROR:
        return StatusJson.STOPPING_ERROR;
      case DELETED:
        return StatusJson.DELETED;
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
        return PipelineStatus.STOPPED;
      case STOPPING:
        return PipelineStatus.STOPPING;
      case RUNNING:
        return PipelineStatus.RUNNING;
      case RUN_ERROR:
        return PipelineStatus.RUN_ERROR;
      case FINISHED:
        return PipelineStatus.FINISHED;
      case CONNECTING:
        return PipelineStatus.CONNECTING;
      case CONNECT_ERROR:
        return PipelineStatus.CONNECT_ERROR;
      case DISCONNECTED:
        return PipelineStatus.DISCONNECTED;
      case DISCONNECTING:
        return PipelineStatus.DISCONNECTING;
      case EDITED:
        return PipelineStatus.EDITED;
      case FINISHING:
        return PipelineStatus.FINISHING;
      case KILLED:
        return PipelineStatus.KILLED;
      case RUNNING_ERROR:
        return PipelineStatus.RUNNING_ERROR;
      case STARTING:
        return PipelineStatus.STARTING;
      case START_ERROR:
        return PipelineStatus.START_ERROR;
      case STARTING_ERROR:
        return PipelineStatus.STARTING_ERROR;
      case RETRY:
        return PipelineStatus.RETRY;
      case STOP_ERROR:
        return PipelineStatus.STOP_ERROR;
      case STOPPING_ERROR:
        return PipelineStatus.STOPPING_ERROR;
      case DELETED:
        return PipelineStatus.DELETED;
      default:
        throw new IllegalArgumentException("Unrecognized state");
    }
  }

  public static MetricElementJson wrapMetricElement(MetricElement metricElement) {
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
      case HISTOGRAM_P50:
        return MetricElementJson.HISTOGRAM_P50;
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
      case LAST_BATCH_INPUT_RECORDS_COUNT:
        return MetricElementJson.LAST_BATCH_INPUT_RECORDS_COUNT;
      case LAST_BATCH_OUTPUT_RECORDS_COUNT:
        return MetricElementJson.LAST_BATCH_OUTPUT_RECORDS_COUNT;
      case LAST_BATCH_ERROR_RECORDS_COUNT:
        return MetricElementJson.LAST_BATCH_ERROR_RECORDS_COUNT;
      case LAST_BATCH_ERROR_MESSAGES_COUNT:
        return MetricElementJson.LAST_BATCH_ERROR_MESSAGES_COUNT;

      default:
        throw new IllegalArgumentException("Unrecognized metric element");
    }
  }

  public static MetricElement unwrapMetricElement(MetricElementJson metricElementJson) {
    if(metricElementJson == null) {
      return null;
    }
    switch(metricElementJson) {
      //Related to Counters
      case COUNTER_COUNT:
        return MetricElement.COUNTER_COUNT;

      //Related to Histogram
      case HISTOGRAM_COUNT:
        return MetricElement.HISTOGRAM_COUNT;
      case HISTOGRAM_MAX:
        return MetricElement.HISTOGRAM_MAX;
      case HISTOGRAM_MIN:
        return MetricElement.HISTOGRAM_MIN;
      case HISTOGRAM_MEAN:
        return MetricElement.HISTOGRAM_MEAN;
      case HISTOGRAM_MEDIAN:
        return MetricElement.HISTOGRAM_MEDIAN;
      case HISTOGRAM_P50:
        return MetricElement.HISTOGRAM_P50;
      case HISTOGRAM_P75:
        return MetricElement.HISTOGRAM_P75;
      case HISTOGRAM_P95:
        return MetricElement.HISTOGRAM_P95;
      case HISTOGRAM_P98:
        return MetricElement.HISTOGRAM_P98;
      case HISTOGRAM_P99:
        return MetricElement.HISTOGRAM_P99;
      case HISTOGRAM_P999:
        return MetricElement.HISTOGRAM_P999;
      case HISTOGRAM_STD_DEV:
        return MetricElement.HISTOGRAM_STD_DEV;

      //Meters
      case METER_COUNT:
        return MetricElement.METER_COUNT;
      case METER_M1_RATE:
        return MetricElement.METER_M1_RATE;
      case METER_M5_RATE:
        return MetricElement.METER_M5_RATE;
      case METER_M15_RATE:
        return MetricElement.METER_M15_RATE;
      case METER_M30_RATE:
        return MetricElement.METER_M30_RATE;
      case METER_H1_RATE:
        return MetricElement.METER_H1_RATE;
      case METER_H6_RATE:
        return MetricElement.METER_H6_RATE;
      case METER_H12_RATE:
        return MetricElement.METER_H12_RATE;
      case METER_H24_RATE:
        return MetricElement.METER_H24_RATE;
      case METER_MEAN_RATE:
        return MetricElement.METER_MEAN_RATE;

      //Timer
      case TIMER_COUNT:
        return MetricElement.TIMER_COUNT;
      case TIMER_MAX:
        return MetricElement.TIMER_MAX;
      case TIMER_MIN:
        return MetricElement.TIMER_MIN;
      case TIMER_MEAN:
        return MetricElement.TIMER_MEAN;
      case TIMER_P50:
        return MetricElement.TIMER_P50;
      case TIMER_P75:
        return MetricElement.TIMER_P75;
      case TIMER_P95:
        return MetricElement.TIMER_P95;
      case TIMER_P98:
        return MetricElement.TIMER_P98;
      case TIMER_P99:
        return MetricElement.TIMER_P99;
      case TIMER_P999:
        return MetricElement.TIMER_P999;
      case TIMER_STD_DEV:
        return MetricElement.TIMER_STD_DEV;
      case TIMER_M1_RATE:
        return MetricElement.TIMER_M1_RATE;
      case TIMER_M5_RATE:
        return MetricElement.TIMER_M5_RATE;
      case TIMER_M15_RATE:
        return MetricElement.TIMER_M15_RATE;
      case TIMER_MEAN_RATE:
        return MetricElement.TIMER_MEAN_RATE;

      //Gauge
      case CURRENT_BATCH_AGE:
        return MetricElement.CURRENT_BATCH_AGE;
      case TIME_IN_CURRENT_STAGE:
        return MetricElement.TIME_IN_CURRENT_STAGE;
      case TIME_OF_LAST_RECEIVED_RECORD:
        return MetricElement.TIME_OF_LAST_RECEIVED_RECORD;
      case LAST_BATCH_INPUT_RECORDS_COUNT:
        return MetricElement.LAST_BATCH_INPUT_RECORDS_COUNT;
      case LAST_BATCH_OUTPUT_RECORDS_COUNT:
        return MetricElement.LAST_BATCH_OUTPUT_RECORDS_COUNT;
      case LAST_BATCH_ERROR_RECORDS_COUNT:
        return MetricElement.LAST_BATCH_ERROR_RECORDS_COUNT;
      case LAST_BATCH_ERROR_MESSAGES_COUNT:
        return MetricElement.LAST_BATCH_ERROR_MESSAGES_COUNT;

      default:
        throw new IllegalArgumentException("Unrecognized metric element");
    }
  }

  public static MetricTypeJson wrapMetricType(MetricType metricType) {
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

  public static MetricType unwrapMetricType(MetricTypeJson metricType) {
    if(metricType == null) {
      return null;
    }
    switch(metricType) {
      case GAUGE:
        return MetricType.GAUGE;
      case HISTOGRAM:
        return MetricType.HISTOGRAM;
      case TIMER:
        return MetricType.TIMER;
      case COUNTER:
        return MetricType.COUNTER;
      case METER:
        return MetricType.METER;
      default:
        throw new IllegalArgumentException("Unrecognized metric type");
    }
  }

  public static ThresholdTypeJson wrapThresholdType(ThresholdType thresholdType) {
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

  public static ThresholdType unwrapThresholdType(ThresholdTypeJson thresholdTypeJson) {
    if(thresholdTypeJson == null) {
      return null;
    }
    switch (thresholdTypeJson) {
      case COUNT:
        return ThresholdType.COUNT;
      case PERCENTAGE:
        return ThresholdType.PERCENTAGE;
      default:
        throw new IllegalArgumentException("Unrecognized metric type");
    }
  }

  public static ModelTypeJson wrapModelType(ModelType modelType) {
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

  public static ModelType unwrapModelType(ModelTypeJson modelTypeJson) {
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

  public static StageTypeJson wrapStageType(StageType stageType) {
    if(stageType == null) {
      return null;
    }
    switch (stageType) {
      case TARGET:
        return StageTypeJson.TARGET;
      case SOURCE:
        return StageTypeJson.SOURCE;
      case EXECUTOR:
        return StageTypeJson.EXECUTOR;
      case PROCESSOR:
        return StageTypeJson.PROCESSOR;
      default:
        throw new IllegalArgumentException("Unrecognized model type");
    }
  }

  public static StageType unwrapStageType(StageTypeJson stageTypeJson) {
    if(stageTypeJson == null) {
      return null;
    }
    switch (stageTypeJson) {
      case TARGET:
        return StageType.TARGET;
      case SOURCE:
        return StageType.SOURCE;
      case EXECUTOR:
        return StageType.EXECUTOR;
      case PROCESSOR:
        return StageType.PROCESSOR;
      default:
        throw new IllegalArgumentException("Unrecognized model type");
    }
  }

  public static CallbackInfoJson wrapCallbackInfo(com.streamsets.datacollector.callback.CallbackInfo callbackInfo) {
    if(callbackInfo == null) {
      return null;
    }
    return new CallbackInfoJson(callbackInfo);
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
      case EDGE:
        return ExecutionModeJson.EDGE;
      case EMR_BATCH:
        return ExecutionModeJson.EMR_BATCH;
      case BATCH:
        return ExecutionModeJson.BATCH;
      case STREAMING:
        return ExecutionModeJson.STREAMING;
      default:
        throw new IllegalArgumentException("Unrecognized execution mode: " + executionMode);
    }
  }

  @SuppressWarnings("deprecation")
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
      case EDGE:
        return ExecutionMode.EDGE;
      case EMR_BATCH:
        return ExecutionMode.EMR_BATCH;
      case BATCH:
        return ExecutionMode.BATCH;
      case STREAMING:
        return ExecutionMode.STREAMING;
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

  public static List<SupportBundleContentDefinitionJson> wrapSupportBundleDefinitions(Collection<BundleContentGeneratorDefinition> definitions) {
    List<SupportBundleContentDefinitionJson> json = new ArrayList<>(definitions.size());
    for(BundleContentGeneratorDefinition def : definitions) {
      json.add(new SupportBundleContentDefinitionJson(def));
    }
    return json;
  }

  public static List<ServiceDependencyDefinitionJson> wrapServiceDependencyDefinitions(List<ServiceDependencyDefinition> services) {
    return services.stream()
      .map(ServiceDependencyDefinitionJson::new)
      .collect(Collectors.toList());
  }

  public static List<ServiceConfigurationJson> wrapServiceConfiguration(List<ServiceConfiguration> services) {
    if (null == services) {
      return null;
    }
    return services.stream()
      .map(ServiceConfigurationJson::new)
      .collect(Collectors.toList());
  }

  public static List<ServiceConfiguration> unwrapServiceConfiguration(List<ServiceConfigurationJson> services) {
    if(services == null) {
      return null;
    }

    return services.stream()
      .map(ServiceConfigurationJson::getServiceConfiguration)
      .collect(Collectors.toList());
  }

  public static List<String> wrapHideStage(List<HideStage.Type> hideStage) {
    if(hideStage == null) {
      return null;
    }

    return hideStage.stream()
      .map(HideStage.Type::name)
      .collect(Collectors.toList());
  }

  public static List<AntennaDoctorMessageJson> wrapAntennaDoctorMessages(List<AntennaDoctorMessage> messages) {
    if(messages == null) {
      return null;
    }

    return messages.stream().map(AntennaDoctorMessageJson::new).collect(Collectors.toList());
  }

  public static List<AntennaDoctorMessage> unwrapAntennaDoctorMessages(List<AntennaDoctorMessageJson> messages) {
    if(messages == null) {
      return null;
    }

    return messages.stream()
        .map(m -> new AntennaDoctorMessage(m.getSummary(), m.getDescription()))
        .collect(Collectors.toList());
  }

  public static ConnectionConfigurationJson wrapConnectionConfiguration(
      ConnectionConfiguration connectionConfiguration
  ) {
    if(connectionConfiguration == null) {
      return null;
    }
    return new ConnectionConfigurationJson(connectionConfiguration);
  }

  public static ConnectionConfiguration unwrapConnectionConfiguration(
      ConnectionConfigurationJson connectionConfigurationJson
  ) {
    if(connectionConfigurationJson == null) {
      return null;
    }
    return connectionConfigurationJson.getConnectionConfiguration();
  }
}
