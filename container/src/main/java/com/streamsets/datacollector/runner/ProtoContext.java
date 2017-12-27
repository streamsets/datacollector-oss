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
package com.streamsets.datacollector.runner;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.el.ELEvaluator;
import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.util.ElUtil;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared context for both Service and Stage.
 */
public abstract class ProtoContext implements ProtoConfigurableEntity.Context {

  private static final String CUSTOM_METRICS_PREFIX = "custom.";

  private final Map<String, Class<?>[]> configToElDefMap;
  private final Map<String, Object> constants;
  protected final MetricRegistry metrics;
  protected final String pipelineId;
  protected final String rev;
  protected final String stageInstanceName;
  protected final String serviceInstanceName;
  protected final String resourcesDir;

  protected ProtoContext(
      Map<String, Class<?>[]> configToElDefMap,
      Map<String, Object> constants,
      MetricRegistry metrics,
      String pipelineId,
      String rev,
      String stageInstanceName,
      String serviceInstanceName,
      String resourcesDir
  ) {
    this.configToElDefMap = configToElDefMap;
    this.constants = constants;
    this.metrics = metrics;
    this.pipelineId = pipelineId;
    this.rev = rev;
    this.stageInstanceName = stageInstanceName;
    this.serviceInstanceName = serviceInstanceName;
    this.resourcesDir = resourcesDir;
  }

  protected static Map<String, Class<?>[]> getConfigToElDefMap(List<ConfigDefinition> configs) {
    Map<String, Class<?>[]> configToElDefMap = new HashMap<>();
    for(ConfigDefinition configDefinition : configs) {
      configToElDefMap.put(configDefinition.getFieldName(),
        ElUtil.getElDefClassArray(configDefinition.getElDefs()));
      if(configDefinition.getModel() != null && configDefinition.getModel().getConfigDefinitions() != null) {
        for(ConfigDefinition configDef : configDefinition.getModel().getConfigDefinitions()) {
          configToElDefMap.put(configDef.getFieldName(),
            ElUtil.getElDefClassArray(configDef.getElDefs()));
        }
      }
    }

    return configToElDefMap;
  }

  private static class ConfigIssueImpl extends Issue implements ConfigIssue {
    public ConfigIssueImpl(
        String stageName,
        String serviceName,
        String configGroup,
        String configName,
        ErrorCode errorCode,
        Object... args
    ) {
      super(stageName, serviceName, configGroup, configName, errorCode, args);
    }
  }

  private static final Object[] NULL_ONE_ARG = {null};


  @Override
  public String getResourcesDirectory() {
    return resourcesDir;
  }

  @Override
  public Record createRecord(String recordSourceId) {
    return new RecordImpl(stageInstanceName, recordSourceId, null, null);
  }

  @Override
  public Record createRecord(String recordSourceId, byte[] raw, String rawMime) {
    return new RecordImpl(stageInstanceName, recordSourceId, raw, rawMime);
  }

  @Override
  public Map<String, Object> getPipelineConstants() {
    return constants;
  }

  @Override
  public ConfigIssue createConfigIssue(
    String configGroup,
    String configName,
    ErrorCode errorCode,
    Object... args
  ) {
    Preconditions.checkNotNull(errorCode, "errorCode cannot be null");
    args = (args != null) ? args.clone() : NULL_ONE_ARG;
    return new ConfigIssueImpl(stageInstanceName, serviceInstanceName, configGroup, configName, errorCode, args);
  }
  @Override
  public MetricRegistry getMetrics() {
    return metrics;
  }

  @Override
  public Timer createTimer(String name) {
    return MetricsConfigurator.createStageTimer(getMetrics(), CUSTOM_METRICS_PREFIX + stageInstanceName + "." + name, pipelineId,
      rev);
  }

  public Timer getTimer(String name) {
    return MetricsConfigurator.getTimer(getMetrics(), CUSTOM_METRICS_PREFIX + stageInstanceName + "." + name);
  }

  @Override
  public Meter createMeter(String name) {
    return MetricsConfigurator.createStageMeter(getMetrics(), CUSTOM_METRICS_PREFIX + stageInstanceName + "." + name, pipelineId,
      rev);
  }

  public Meter getMeter(String name) {
    return MetricsConfigurator.getMeter(getMetrics(), CUSTOM_METRICS_PREFIX + stageInstanceName + "." + name);
  }

  @Override
  public Counter createCounter(String name) {
    return MetricsConfigurator.createStageCounter(getMetrics(), CUSTOM_METRICS_PREFIX + stageInstanceName + "." + name, pipelineId,
      rev);
  }

  public Counter getCounter(String name) {
    return MetricsConfigurator.getCounter(getMetrics(), CUSTOM_METRICS_PREFIX + stageInstanceName + "." + name);
  }

  @Override
  public Histogram createHistogram(String name) {
    return MetricsConfigurator.createStageHistogram5Min(getMetrics(), CUSTOM_METRICS_PREFIX + stageInstanceName + "." + name, pipelineId, rev);
  }

  @Override
  public Histogram getHistogram(String name) {
    return MetricsConfigurator.getHistogram(getMetrics(), CUSTOM_METRICS_PREFIX + stageInstanceName + "." + name);
  }

  @Override
  public Gauge<Map<String, Object>> createGauge(String name) {
    return MetricsConfigurator.createStageGauge(getMetrics(), CUSTOM_METRICS_PREFIX + stageInstanceName + "." + name, null, pipelineId, rev);
  }

  @Override
  public Gauge<Map<String, Object>> createGauge(String name, Comparator<String> comparator) {
    return MetricsConfigurator.createStageGauge(getMetrics(), CUSTOM_METRICS_PREFIX + stageInstanceName + "." + name, comparator, pipelineId, rev);
  }

  @Override
  public Gauge<Map<String, Object>> getGauge(String name) {
    return MetricsConfigurator.getGauge(getMetrics(), CUSTOM_METRICS_PREFIX + stageInstanceName + "." + name);
  }

  // ELContext

  @Override
  public void parseEL(String el) throws ELEvalException {
    ELEvaluator.parseEL(el);
  }

  @Override
  public ELVars createELVars() {
    return new ELVariables(constants);
  }

  @Override
  public ELEval createELEval(String configName) {
    return createELEval(configName, configToElDefMap.get(configName));
  }

  @Override
  public ELEval createELEval(String configName, Class<?>... elDefClasses) {
    List<Class> classes = new ArrayList<>();
    Class[] configClasses = configToElDefMap.get(configName);
    if (configClasses != null) {
      Collections.addAll(classes, configClasses);
    }
    if (elDefClasses != null) {
      Collections.addAll(classes, elDefClasses);
    }
    // assert non of the EL functions is implicit only
    return new ELEvaluator(configName, true, constants, classes.toArray(new Class[classes.size()]));
  }

}
