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
package com.streamsets.datacollector.runner;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.streamsets.datacollector.blobstore.BlobStoreRuntime;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.lineage.LineagePublisherDelegator;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.BlobStore;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.interceptor.Interceptor;
import com.streamsets.pipeline.api.interceptor.InterceptorCreator;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class InterceptorContext implements Interceptor.Context {
  private static final String CUSTOM_METRICS_PREFIX = "interceptor.custom.";

  private final InterceptorCreator.InterceptorType type;
  private final StageLibraryTask stageLibrary;
  private final String pipelineId;
  private final String pipelineTitle;
  private final String rev;
  private final String sdcId;
  private final Stage.UserContext userContext;
  private final boolean isPreview;
  private final MetricRegistry metrics;
  private final ExecutionMode executionMode;
  private final DeliveryGuarantee deliveryGuarantee;
  private final BuildInfo buildInfo;
  private final RuntimeInfo runtimeInfo;
  private final EmailSender emailSender;
  private final long startTime;
  private final LineagePublisherDelegator lineagePublisherDelegator;
  private final BlobStore blobStore;
  private final Configuration configuration;
  private final String stageInstanceName;
  private final String metricName;

  /**
   * Flag to configure if createStage method should be allowed or not.
   */
  private boolean allowCreateStage = false;
  public void setAllowCreateStage(boolean allowCreateStage) {
    this.allowCreateStage = allowCreateStage;
  }

  /**
   * List of issues for dependent stages.
   */
  private List issues = new ArrayList<>();
  public List<Issue> getIssues() {
    return issues;
  }

  /**
   * Return list of all created stages
   */
  private List<DetachedStageRuntime> stageRuntimes = new ArrayList<>();
  public List<DetachedStageRuntime> getStageRuntimes() {
    return Collections.unmodifiableList(stageRuntimes);
  }

  /**
   * Classloader that is used by the container module.
   */
  private final ClassLoader containerClassLoader;

  public InterceptorContext(
    InterceptorCreator.InterceptorType type,
    BlobStore blobStore,
    Configuration configuration,
    String stageInstanceName,
    String metricName,
    StageLibraryTask stageLibrary,
    String pipelineId,
    String pipelineTitle,
    String rev,
    String sdcId,
    boolean isPreview,
    Stage.UserContext userContext,
    MetricRegistry metrics,
    ExecutionMode executionMode,
    DeliveryGuarantee deliveryGuarantee,
    BuildInfo buildInfo,
    RuntimeInfo runtimeInfo,
    EmailSender emailSender,
    long startTime,
    LineagePublisherDelegator lineagePublisherDelegator
  ) {
    this.containerClassLoader = Thread.currentThread().getContextClassLoader();
    this.type = type;
    this.blobStore = new BlobStoreRuntime(containerClassLoader, blobStore);
    this.configuration = configuration;
    this.stageInstanceName = stageInstanceName;
    this.metricName = metricName;
    this.stageLibrary = stageLibrary;
    this.pipelineId = pipelineId;
    this.pipelineTitle = pipelineTitle;
    this.sdcId = sdcId;
    this.isPreview = isPreview;
    this.rev = rev;
    this.userContext = userContext;
    this.metrics = metrics;
    this.executionMode = executionMode;
    this.deliveryGuarantee = deliveryGuarantee;
    this.buildInfo = buildInfo;
    this.runtimeInfo = runtimeInfo;
    this.emailSender = emailSender;
    this.startTime = startTime;
    this.lineagePublisherDelegator = lineagePublisherDelegator;
    PipelineBeanCreator.prepareForConnections(configuration, runtimeInfo);
  }

  @Override
  public String getPipelineId() {
    return pipelineId;
  }

  public String getRev() {
    return rev;
  }

  @Override
  public String getStageInstanceName() {
    return stageInstanceName;
  }

  @Override
  public MetricRegistry getMetrics() {
    return metrics;
  }

  @Override
  public Timer createTimer(String name) {
    return MetricsConfigurator.createStageTimer(getMetrics(), createMetricName(name), pipelineId, rev);
  }

  public Timer getTimer(String name) {
    return MetricsConfigurator.getTimer(getMetrics(), createMetricName(name));
  }

  @Override
  public Meter createMeter(String name) {
    return MetricsConfigurator.createStageMeter(getMetrics(), createMetricName(name), pipelineId, rev);
  }

  public Meter getMeter(String name) {
    return MetricsConfigurator.getMeter(getMetrics(), createMetricName(name));
  }

  @Override
  public Counter createCounter(String name) {
    return MetricsConfigurator.createStageCounter(getMetrics(), createMetricName(name), pipelineId, rev);
  }

  public Counter getCounter(String name) {
    return MetricsConfigurator.getCounter(getMetrics(), createMetricName(name));
  }

  @Override
  public Histogram createHistogram(String name) {
    return MetricsConfigurator.createStageHistogram5Min(getMetrics(), createMetricName(name), pipelineId, rev);
  }

  @Override
  public Histogram getHistogram(String name) {
    return MetricsConfigurator.getHistogram(getMetrics(), createMetricName(name));
  }

  @Override
  public Gauge<Map<String, Object>> createGauge(String name) {
    return MetricsConfigurator.createStageGauge(getMetrics(), createMetricName(name), null, pipelineId, rev);
  }

  @Override
  public Gauge<Map<String, Object>> createGauge(String name, Comparator<String> comparator) {
    return MetricsConfigurator.createStageGauge(getMetrics(), createMetricName(name), comparator, pipelineId, rev);
  }

  @Override
  public Gauge<Map<String, Object>> getGauge(String name) {
    return MetricsConfigurator.getGauge(getMetrics(), createMetricName(name));
  }

  private String createMetricName(String name) {
    return CUSTOM_METRICS_PREFIX + type.name() + "." + stageInstanceName + "." + metricName + "." + name;
  }

  @Override
  public ConfigIssue createConfigIssue(ErrorCode errorCode, Object... args) {
    Preconditions.checkNotNull(errorCode, "errorCode cannot be null");
    args = (args != null) ? args.clone() : ProtoContext.NULL_ONE_ARG;
    return new ProtoContext.ConfigIssueImpl(stageInstanceName, null, null, null, errorCode, args);
  }

  @Override
  public String getConfig(String configName) {
    return configuration.get(configName, null);
  }

  @Override
  public com.streamsets.pipeline.api.Configuration getConfiguration() {
    return configuration;
  }

  @Override
  public BlobStore getBlobStore() {
    return blobStore;
  }

  @Override
  public <S> S createStage(String jsonDefinition, Class<S> klass) {
    if(!allowCreateStage) {
      throw new IllegalStateException("Method createStage can only be called during initialization phase!");
    }

    if(!DetachedStageRuntime.supports(klass)) {
      throw new IllegalArgumentException("This runtime does not support " + klass.getName());
    }

    ClassLoader interceptorClassLoader = Thread.currentThread().getContextClassLoader();

    return AccessController.doPrivileged((PrivilegedAction<S>) () -> {
      try {
        Thread.currentThread().setContextClassLoader(containerClassLoader);

        if (Strings.isNullOrEmpty(jsonDefinition)) {
          return null;
        }

        DetachedStageRuntime stageRuntime = DetachedStage.get().createDetachedStage(
          jsonDefinition,
          stageLibrary,
          pipelineId,
          pipelineTitle,
          rev,
          userContext,
          metrics,
          executionMode,
          deliveryGuarantee,
          buildInfo,
          runtimeInfo,
          emailSender,
          configuration,
          startTime,
          lineagePublisherDelegator,
          klass,
          issues
        );
        if (stageRuntime == null) {
          return null;
        }

        stageRuntimes.add(stageRuntime);

        List localIssues = stageRuntime.runInit();
        if (!localIssues.isEmpty()) {
          issues.addAll(localIssues);
          return null;
        }

        return (S) stageRuntime;
      } finally {
        Thread.currentThread().setContextClassLoader(interceptorClassLoader);
      }
    });
  }

  @Override
  public boolean isPreview() {
    return isPreview;
  }

  @Override
  public String getSdcId() {
    return sdcId;
  }
}
