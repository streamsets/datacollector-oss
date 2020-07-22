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
package com.streamsets.datacollector.execution.runner.common.dagger;


import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.blobstore.BlobStoreTask;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.SnapshotStore;
import com.streamsets.datacollector.execution.alerts.AlertManager;
import com.streamsets.datacollector.execution.alerts.EmailNotifier;
import com.streamsets.datacollector.execution.metrics.MetricsEventRunnable;
import com.streamsets.datacollector.execution.runner.common.DataObserverRunnable;
import com.streamsets.datacollector.execution.runner.common.MetricObserverRunnable;
import com.streamsets.datacollector.execution.runner.common.MetricsObserverRunner;
import com.streamsets.datacollector.execution.runner.common.ProductionObserver;
import com.streamsets.datacollector.execution.runner.common.ProductionPipelineBuilder;
import com.streamsets.datacollector.execution.runner.common.ProductionPipelineRunnable;
import com.streamsets.datacollector.execution.runner.common.ProductionPipelineRunner;
import com.streamsets.datacollector.execution.runner.common.RulesConfigLoader;
import com.streamsets.datacollector.execution.runner.common.ThreadHealthReporter;
import com.streamsets.datacollector.execution.snapshot.file.FileSnapshotStore;
import com.streamsets.datacollector.lineage.LineagePublisherTask;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsModule;
import com.streamsets.datacollector.runner.Observer;
import com.streamsets.datacollector.runner.PipelineRunner;
import com.streamsets.datacollector.runner.SourceOffsetTracker;
import com.streamsets.datacollector.runner.production.ProductionSourceOffsetTracker;
import com.streamsets.datacollector.runner.production.RulesConfigLoaderRunnable;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.usagestats.StatsCollector;
import com.streamsets.datacollector.util.Configuration;
import dagger.Module;
import dagger.Provides;

import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Map;

@Module(injects = {EmailNotifier.class, EmailSender.class, AlertManager.class, Observer.class, RulesConfigLoader.class,
  ThreadHealthReporter.class, DataObserverRunnable.class, RulesConfigLoaderRunnable.class, MetricObserverRunnable.class,
  ProductionPipelineBuilder.class, PipelineRunner.class, MetricsEventRunnable.class},
  library = true, complete = false, includes = {MetricsModule.class})
public class PipelineProviderModule {

  private final String pipelineId;
  private final String title;
  private final String rev;
  private final boolean statsAggregationEnabled;
  private final Map<String, Object> resolvedParameters;

  public PipelineProviderModule(
      String pipelineId,
      String title,
      String rev,
      boolean statsAggregationEnabled,
      Map<String, Object> resolvedParameters
  ) {
    this.pipelineId = pipelineId;
    this.title = title;
    this.rev = rev;
    this.statsAggregationEnabled = statsAggregationEnabled;
    this.resolvedParameters = resolvedParameters;
  }

  @Provides @Named("name")
  public String provideName() {
    return pipelineId;
  }

  @Provides @Named("rev")
  public String provideRev() {
    return rev;
  }

  @Provides @Singleton
  public EmailSender provideEmailSender(Configuration configuration) {
    return new EmailSender(configuration);
  }

  @Provides @Singleton
  public AlertManager provideAlertManager(
      @Named("name") String name,
      @Named("rev") String rev,
      EmailSender emailSender,
      MetricRegistry metricRegistry,
      RuntimeInfo runtimeInfo,
      EventListenerManager eventListenerManager
  ) {
    return new AlertManager(name, title, rev, emailSender, metricRegistry, runtimeInfo, eventListenerManager);
  }

  @Provides @Singleton
  public MetricsObserverRunner provideMetricsObserverRunner(
      MetricRegistry metricRegistry,
      AlertManager alertManager,
      Configuration configuration,
      RuntimeInfo runtimeInfo
  ) {
    return new MetricsObserverRunner(
        pipelineId,
        rev,
        statsAggregationEnabled,
        metricRegistry,
        alertManager,
        resolvedParameters,
        configuration,
        runtimeInfo
    );
  }

  @Provides @Singleton
  public Observer provideProductionObserver(ProductionObserver productionObserver) {
    return productionObserver;
  }

  @Provides @Singleton
  public SourceOffsetTracker provideProductionSourceOffsetTracker(
      ProductionSourceOffsetTracker productionSourceOffsetTracker
  ) {
    return productionSourceOffsetTracker;
  }

  @Provides @Singleton
  public RulesConfigLoader provideRulesConfigLoader(
      @Named("name") String name,
      @Named("rev") String rev,
      PipelineStoreTask pipelineStoreTask,
      Configuration configuration
  ) {
    return new RulesConfigLoader(name, rev, pipelineStoreTask, configuration);
  }

  @Provides @Singleton
  public ThreadHealthReporter provideThreadHealthReporter(MetricRegistry metricRegistry) {
    ThreadHealthReporter threadHealthReporter = new ThreadHealthReporter(pipelineId, rev, metricRegistry);
    threadHealthReporter.register(RulesConfigLoaderRunnable.RUNNABLE_NAME);
    threadHealthReporter.register(MetricObserverRunnable.RUNNABLE_NAME);
    threadHealthReporter.register(DataObserverRunnable.RUNNABLE_NAME);
    threadHealthReporter.register(ProductionPipelineRunnable.RUNNABLE_NAME);
    threadHealthReporter.register(MetricsEventRunnable.RUNNABLE_NAME);
    return threadHealthReporter;
  }

  @Provides @Singleton
  public RulesConfigLoaderRunnable provideRulesConfigLoaderRunnable(
      ThreadHealthReporter threadHealthReporter,
      RulesConfigLoader rulesConfigLoader,
      Observer observer
  ) {
    return new RulesConfigLoaderRunnable(threadHealthReporter, rulesConfigLoader, observer);
  }

  @Provides @Singleton
  public MetricObserverRunnable provideMetricObserverRunnable(
      ThreadHealthReporter threadHealthReporter,
      MetricsObserverRunner metricsObserverRunner
  ) {
    return new MetricObserverRunnable(threadHealthReporter, metricsObserverRunner);
  }

  @Provides @Singleton
  public DataObserverRunnable provideDataObserverRunnable(
      ThreadHealthReporter threadHealthReporter,
      MetricRegistry metricRegistry,
      AlertManager alertManager,
      Configuration configuration,
      RuntimeInfo runtimeInfo
  ) {
    return new DataObserverRunnable(
        pipelineId,
        rev,
        threadHealthReporter,
        metricRegistry,
        alertManager,
        configuration,
        runtimeInfo,
        resolvedParameters
    );
  }


  @Provides @Singleton
  public PipelineRunner provideProductionPipelineRunner(ProductionPipelineRunner productionPipelineRunner) {
    return productionPipelineRunner;
  }

  @Provides @Singleton
  public ProductionPipelineBuilder provideProductionPipelineBuilder(
    @Named("name") String name,
    @Named("rev") String rev,
    Configuration configuration,
    RuntimeInfo runtimeInfo,
    BuildInfo buildInfo,
    StageLibraryTask stageLib,
    PipelineRunner runner,
    Observer observer,
    BlobStoreTask blobStoreTask,
    LineagePublisherTask lineagePublisherTask,
    StatsCollector statsCollector
  ) {
    return new ProductionPipelineBuilder(
      name,
      rev,
      configuration,
      runtimeInfo,
      buildInfo,
      stageLib,
      (ProductionPipelineRunner)runner,
      observer,
      blobStoreTask,
      lineagePublisherTask,
      statsCollector
    );
  }

  @Provides @Singleton
  public SnapshotStore provideSnapshotStore(FileSnapshotStore fileSnapshotStore) {
    return fileSnapshotStore;
  }

  @Provides @Singleton
  public MetricsEventRunnable provideMetricsEventRunnable(
      @Named("name") String name,
      @Named("rev") String rev,
      Configuration configuration,
      PipelineStateStore pipelineStateStore,
      MetricRegistry metricRegistry,
      ThreadHealthReporter threadHealthReporter,
      EventListenerManager eventListenerManager,
      RuntimeInfo runtimeInfo
  ) {
    return new MetricsEventRunnable(
        name,
        rev,
        configuration,
        pipelineStateStore,
        threadHealthReporter,
        eventListenerManager,
        metricRegistry,
        null,
        runtimeInfo
    );
  }

}
