/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.observerstore.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.streamsets.pipeline.config.AlertDefinition;
import com.streamsets.pipeline.config.CounterDefinition;
import com.streamsets.pipeline.config.MetricsAlertDefinition;
import com.streamsets.pipeline.config.SamplingDefinition;
import com.streamsets.pipeline.io.DataStore;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.observerstore.ObserverStore;
import com.streamsets.pipeline.util.PipelineDirectoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class FileObserverStore implements ObserverStore {

  private static final Logger LOG = LoggerFactory.getLogger(FileObserverStore.class);

  private static final String ALERTS_FILE = "alerts.json";
  private static final String METRIC_ALERTS_FILE = "metricAlerts.json";
  private static final String SAMPLING_DEFINITIONS_FILE = "samplingDefinitions.json";
  private static final String COUNTERS_FILE = "counters.json";

  private final RuntimeInfo runtimeInfo;

  public FileObserverStore(RuntimeInfo runtimeInfo) {
    this.runtimeInfo = runtimeInfo;
  }

  @Override
  public List<AlertDefinition> storeAlerts(String pipelineName, String rev, List<AlertDefinition> alerts) {
    LOG.trace("Writing alert definitions to '{}'", getAlertsFile(pipelineName, rev).getAbsolutePath());
    try {
      ObjectMapperFactory.get().writeValue(new DataStore(getAlertsFile(pipelineName, rev)).getOutputStream(),
        alerts);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return alerts;
  }

  @Override
  public List<AlertDefinition> retrieveAlerts(String pipelineName, String rev) {
    if(!PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev).exists() ||
      !getAlertsFile(pipelineName, rev).exists()) {
      return Collections.emptyList();
    }
    LOG.trace("Reading alert definitions from '{}'", getAlertsFile(pipelineName, rev).getAbsolutePath());
    try {
      List<AlertDefinition> alerts = ObjectMapperFactory.get().readValue(
        new DataStore(getAlertsFile(pipelineName, rev)).getInputStream(), new TypeReference<List<AlertDefinition>>() {
      });
      return alerts;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<MetricsAlertDefinition> storeMetricAlerts(String pipelineName, String rev,
                                                        List<MetricsAlertDefinition> metricsAlerts) {
    LOG.trace("Writing metric alert definitions to '{}'", getMetricAlertsFile(pipelineName, rev).getAbsolutePath());
    try {
      ObjectMapperFactory.get().writeValue(new DataStore(getMetricAlertsFile(pipelineName, rev)).getOutputStream(),
        metricsAlerts);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return metricsAlerts;
  }

  @Override
  public List<MetricsAlertDefinition> retrieveMetricAlerts(String pipelineName, String rev) {
    if(!PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev).exists() ||
      !getMetricAlertsFile(pipelineName, rev).exists()) {
      return Collections.emptyList();
    }
    LOG.trace("Reading metric alert definitions from '{}'", getMetricAlertsFile(pipelineName, rev).getAbsolutePath());
    try {
      List<MetricsAlertDefinition> metricsAlertDefinitions = ObjectMapperFactory.get().readValue(
        new DataStore(getMetricAlertsFile(pipelineName, rev)).getInputStream(),
        new TypeReference<List<MetricsAlertDefinition>>() {});
      return metricsAlertDefinitions;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<SamplingDefinition> retrieveSamplingDefinitions(String pipelineName, String rev) {
    if(!PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev).exists() ||
      !getSamplingDefinitionsFile(pipelineName, rev).exists()) {
      return Collections.emptyList();
    }
    LOG.trace("Reading sampling definitions from '{}'",
      getSamplingDefinitionsFile(pipelineName, rev).getAbsolutePath());
    try {
      List<SamplingDefinition> samplingDefinitions = ObjectMapperFactory.get().readValue(
        new DataStore(getSamplingDefinitionsFile(pipelineName, rev)).getInputStream(),
        new TypeReference<List<SamplingDefinition>>() {});
      return samplingDefinitions;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<SamplingDefinition> storeSamplingDefinitions(String pipelineName, String rev,
                                                           List<SamplingDefinition> samplingDefinitions) {
    LOG.trace("Writing sampling definitions to '{}'", getSamplingDefinitionsFile(pipelineName, rev).getAbsolutePath());
    try {
      ObjectMapperFactory.get().writeValue(
        new DataStore(getSamplingDefinitionsFile(pipelineName, rev)).getOutputStream(), samplingDefinitions);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return samplingDefinitions;
  }

  @Override
  public List<CounterDefinition> storeCounters(String pipelineName, String rev,
                                               List<CounterDefinition> counterDefinitions) {
    LOG.trace("Writing counter definitions to '{}'", getCountersFile(pipelineName, rev).getAbsolutePath());
    try {
      ObjectMapperFactory.get().writeValue(new DataStore(getCountersFile(pipelineName, rev)).getOutputStream(),
        counterDefinitions);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return counterDefinitions;
  }

  @Override
  public List<CounterDefinition> retrieveCounters(String pipelineName, String rev) {
    if(!PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev).exists() ||
      !getCountersFile(pipelineName, rev).exists()) {
      return Collections.emptyList();
    }
    LOG.trace("Reading counter definitions from '{}'",
      getCountersFile(pipelineName, rev).getAbsolutePath());
    try {
      List<CounterDefinition> counterDefinitions = ObjectMapperFactory.get().readValue(
        new DataStore(getCountersFile(pipelineName, rev)).getInputStream(),
        new TypeReference<List<CounterDefinition>>() {});
      return counterDefinitions;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private File getAlertsFile(String pipelineName, String rev) {
    return new File(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev), ALERTS_FILE);
  }

  private File getMetricAlertsFile(String pipelineName, String rev) {
    return new File(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev), METRIC_ALERTS_FILE);
  }

  private File getSamplingDefinitionsFile(String pipelineName, String rev) {
    return new File(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev), SAMPLING_DEFINITIONS_FILE);
  }

  private File getCountersFile(String pipelineName, String rev) {
    return new File(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev), COUNTERS_FILE);
  }
}
