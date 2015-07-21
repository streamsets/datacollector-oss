/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.runner.common.dagger;


import com.codahale.metrics.MetricRegistry;
import com.streamsets.dc.execution.EventListenerManager;
import com.streamsets.dc.execution.SnapshotStore;
import com.streamsets.dc.execution.alerts.AlertManager;
import com.streamsets.dc.execution.metrics.MetricsEventRunnable;
import com.streamsets.dc.execution.runner.common.DataObserverRunnable;
import com.streamsets.dc.execution.runner.common.MetricObserverRunnable;
import com.streamsets.dc.execution.runner.common.MetricsObserverRunner;
import com.streamsets.dc.execution.runner.common.ProductionObserver;
import com.streamsets.dc.execution.runner.common.ProductionPipelineBuilder;
import com.streamsets.dc.execution.runner.common.ProductionPipelineRunnable;
import com.streamsets.dc.execution.runner.common.ProductionPipelineRunner;
import com.streamsets.dc.execution.snapshot.file.FileSnapshotStore;
import com.streamsets.pipeline.email.EmailSender;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.metrics.MetricsModule;
import com.streamsets.pipeline.runner.Observer;
import com.streamsets.pipeline.runner.PipelineRunner;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.runner.production.ProductionSourceOffsetTracker;
import com.streamsets.pipeline.runner.production.RulesConfigLoader;
import com.streamsets.pipeline.runner.production.RulesConfigLoaderRunnable;
import com.streamsets.pipeline.runner.production.ThreadHealthReporter;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.util.Configuration;
import dagger.Module;
import dagger.Provides;

import javax.inject.Named;
import javax.inject.Singleton;

@Module(injects = {EmailSender.class, AlertManager.class, Observer.class, RulesConfigLoader.class,
  ThreadHealthReporter.class, DataObserverRunnable.class, RulesConfigLoaderRunnable.class, MetricObserverRunnable.class,
  ProductionPipelineBuilder.class, PipelineRunner.class},
  library = true, complete = false, includes = {MetricsModule.class})
public class PipelineProviderModule {

  private final String name;
  private final String rev;

  public PipelineProviderModule(String name, String rev) {
    this.name = name;
    this.rev = rev;
  }

  @Provides @Named("name")
  public String provideName() {
    return name;
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
  public AlertManager provideAlertManager(@Named("name") String name, @Named("rev") String rev, EmailSender emailSender,
                                          MetricRegistry metricRegistry, RuntimeInfo runtimeInfo,
                                          EventListenerManager eventListenerManager) {
    return new AlertManager(name, rev, emailSender, metricRegistry, runtimeInfo, eventListenerManager);
  }

  @Provides @Singleton
  public MetricsObserverRunner provideMetricsObserverRunner(MetricRegistry metricRegistry, AlertManager alertManager) {
    return new MetricsObserverRunner(name, rev, metricRegistry, alertManager);
  }

  @Provides @Singleton
  public Observer provideProductionObserver(ProductionObserver productionObserver) {
    return productionObserver;
  }

  @Provides @Singleton
  public SourceOffsetTracker provideProductionSourceOffsetTracker(ProductionSourceOffsetTracker productionSourceOffsetTracker) {
    return productionSourceOffsetTracker;
  }

  @Provides @Singleton
  public RulesConfigLoader provideRulesConfigLoader(@Named("name") String name, @Named("rev") String rev,
                                                    PipelineStoreTask pipelineStoreTask) {
    return new RulesConfigLoader(name, rev, pipelineStoreTask);
  }

  @Provides @Singleton
  public ThreadHealthReporter provideThreadHealthReporter(MetricRegistry metricRegistry) {
    ThreadHealthReporter threadHealthReporter = new ThreadHealthReporter(name, rev, metricRegistry);
    threadHealthReporter.register(RulesConfigLoaderRunnable.RUNNABLE_NAME);
    threadHealthReporter.register(MetricObserverRunnable.RUNNABLE_NAME);
    threadHealthReporter.register(DataObserverRunnable.RUNNABLE_NAME);
    threadHealthReporter.register(ProductionPipelineRunnable.RUNNABLE_NAME);
    threadHealthReporter.register(MetricsEventRunnable.RUNNABLE_NAME);
    return threadHealthReporter;
  }

  @Provides @Singleton
  public RulesConfigLoaderRunnable provideRulesConfigLoaderRunnable(ThreadHealthReporter threadHealthReporter,
                                                                    RulesConfigLoader rulesConfigLoader,
                                                                    Observer observer) {
    return new RulesConfigLoaderRunnable(threadHealthReporter, rulesConfigLoader, observer);
  }

  @Provides @Singleton
  public MetricObserverRunnable provideMetricObserverRunnable(ThreadHealthReporter threadHealthReporter,
                                                              MetricsObserverRunner metricsObserverRunner) {
    return new MetricObserverRunnable(threadHealthReporter, metricsObserverRunner);
  }

  @Provides @Singleton
  public DataObserverRunnable provideDataObserverRunnable(ThreadHealthReporter threadHealthReporter,
                                                          MetricRegistry metricRegistry, AlertManager alertManager,
                                                          Configuration configuration) {
    return new DataObserverRunnable(name, rev, threadHealthReporter, metricRegistry, alertManager, configuration);
  }


  @Provides @Singleton
  public PipelineRunner provideProductionPipelineRunner(ProductionPipelineRunner productionPipelineRunner) {
    return productionPipelineRunner;
  }

  @Provides @Singleton
  public ProductionPipelineBuilder provideProductionPipelineBuilder(@Named("name") String name,
                                                                    @Named("rev") String rev,
                                                                    RuntimeInfo runtimeInfo, StageLibraryTask stageLib,
                                                                    PipelineRunner runner, Observer observer) {
    return new ProductionPipelineBuilder(name, rev, runtimeInfo, stageLib, (ProductionPipelineRunner)runner, observer);
  }

  @Provides @Singleton
  public SnapshotStore provideSnapshotStore(FileSnapshotStore fileSnapshotStore) {
    return fileSnapshotStore;
  }
}
