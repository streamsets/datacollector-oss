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
import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.el.ELEvaluator;
import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.datacollector.email.EmailException;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.record.io.JsonWriterReaderFactory;
import com.streamsets.datacollector.record.io.RecordWriterReaderFactory;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.ElUtil;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.JsonObjectReader;
import com.streamsets.pipeline.api.ext.JsonRecordWriter;
import com.streamsets.pipeline.api.ext.RecordReader;
import com.streamsets.pipeline.api.ext.RecordWriter;
import com.streamsets.pipeline.api.ext.Sampler;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.sampling.RecordSampler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared context for both Service and Stage.
 */
public abstract class ProtoContext implements ProtoConfigurableEntity.Context, ContextExtensions {

  private static final Logger LOG = LoggerFactory.getLogger(ProtoContext.class);
  private static final String CUSTOM_METRICS_PREFIX = "custom.";
  protected static final String STAGE_CONF_PREFIX = "stage.conf_";
  private static final String SDC_RECORD_SAMPLING_POPULATION_SIZE = "sdc.record.sampling.population.size";
  private static final String SDC_RECORD_SAMPLING_SAMPLE_SIZE = "sdc.record.sampling.sample.size";

  protected final Configuration configuration;
  private final Map<String, Class<?>[]> configToElDefMap;
  private final Map<String, Object> constants;
  private final EmailSender emailSender;
  protected final MetricRegistry metrics;
  protected final String pipelineId;
  protected final String rev;
  private final Sampler sampler;
  protected final String stageInstanceName;
  protected final String serviceInstanceName;
  protected final String resourcesDir;

  protected ProtoContext(
      Configuration configuration,
      Map<String, Class<?>[]> configToElDefMap,
      Map<String, Object> constants,
      EmailSender emailSender,
      MetricRegistry metrics,
      String pipelineId,
      String rev,
      String stageInstanceName,
      StageType stageType,
      String serviceInstanceName,
      String resourcesDir
  ) {
    this.configuration = configuration.getSubSetConfiguration(STAGE_CONF_PREFIX);
    this.configToElDefMap = configToElDefMap;
    this.constants = constants;
    this.emailSender = emailSender;
    this.metrics = metrics;
    this.pipelineId = pipelineId;
    this.rev = rev;
    this.stageInstanceName = stageInstanceName;
    this.serviceInstanceName = serviceInstanceName;
    this.resourcesDir = resourcesDir;

    // Initialize Sampler
    int sampleSize = configuration.get(SDC_RECORD_SAMPLING_SAMPLE_SIZE, 1);
    int populationSize = configuration.get(SDC_RECORD_SAMPLING_POPULATION_SIZE, 10000);
    this.sampler = new RecordSampler(this, stageType == StageType.SOURCE, sampleSize, populationSize);
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

  // ContextExtensions

  @Override
  public RecordReader createRecordReader(
      InputStream inputStream,
      long initialPosition,
      int maxObjectLen
  ) throws IOException {
    return RecordWriterReaderFactory.createRecordReader(inputStream, initialPosition, maxObjectLen);
  }

  @Override
  public RecordWriter createRecordWriter(OutputStream outputStream) throws IOException {
    return RecordWriterReaderFactory.createRecordWriter(this, outputStream);
  }

  @Override
  public void notify(List<String> addresses, String subject, String body) throws StageException {
    try {
      emailSender.send(addresses, subject, body);
    } catch (EmailException e) {
      LOG.error(Utils.format(ContainerError.CONTAINER_01001.getMessage(), e.toString(), e));
      throw new StageException(ContainerError.CONTAINER_01001, e.toString(), e);
    }
  }

  @Override
  public Sampler getSampler() {
    return sampler;
  }

  @Override
  public JsonObjectReader createJsonObjectReader(
      Reader reader,
      long initialPosition,
      Mode mode,
      Class<?> objectClass
  ) throws IOException {
    return JsonWriterReaderFactory.createObjectReader(reader, initialPosition, mode, objectClass);
  }

  @Override
  public JsonObjectReader createJsonObjectReader(
        Reader reader,
        long initialPosition,
        int maxObjectLen,
        Mode mode,
        Class<?> objectClass
  ) throws IOException {
    return JsonWriterReaderFactory.createObjectReader(reader, initialPosition, mode, objectClass, maxObjectLen);
  }

  @Override
  public JsonRecordWriter createJsonRecordWriter(
      Writer writer,
      Mode mode
  ) throws IOException {
    return JsonWriterReaderFactory.createRecordWriter(writer, mode);
  }
}
