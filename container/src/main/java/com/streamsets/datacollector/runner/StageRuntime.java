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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.streamsets.datacollector.antennadoctor.AntennaDoctor;
import com.streamsets.datacollector.antennadoctor.engine.context.AntennaDoctorStageContext;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.creation.PipelineBean;
import com.streamsets.datacollector.creation.StageBean;
import com.streamsets.datacollector.runner.production.ReportErrorDelegate;
import com.streamsets.datacollector.util.LambdaUtil;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.SourceResponseSink;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.impl.CreateByRef;
import com.streamsets.pipeline.api.impl.Utils;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

public class StageRuntime implements PushSourceContextDelegate {
  private final PipelineBean pipelineBean;
  private final StageDefinition def;
  private final StageConfiguration conf;
  private final StageBean stageBean;
  private final Stage.Info info;
  private final Collection<ServiceRuntime> services;
  private final List<InterceptorRuntime> preInterceptors;
  private final List<InterceptorRuntime> postInterceptors;
  private final AntennaDoctor antennaDoctor;
  private final AntennaDoctorStageContext antennaDoctorContext;
  private StageContext context;
  private volatile long runnerThread;

  /**
   * In case of PushSource, the delegate that needs to be called for it's callbacks.
   */
  private PushSourceContextDelegate pushSourceContextDelegate;

  /**
   * Optional error delegate.
   *
   * If not set, then the ErrorSink will be used instead.
   */
  private ReportErrorDelegate reportErrorDelegate;

  /**
   * Classloader of the main application persisted on each execute() and destroy() call.
   */
  private ClassLoader mainClassLoader;

  public StageRuntime(
    PipelineBean pipelineBean,
    final StageBean stageBean,
    Collection<ServiceRuntime> services,
    List<InterceptorRuntime> preInterceptors,
    List<InterceptorRuntime> postInterceptors,
    AntennaDoctor antennaDoctor,
    AntennaDoctorStageContext antennaDoctorContext
  ) {
    this.pipelineBean = pipelineBean;
    this.def = stageBean.getDefinition();
    this.stageBean = stageBean;
    this.conf = stageBean.getConfiguration();
    String label = Optional.ofNullable(conf.getUiInfo().get("label")).orElse("").toString();
    this.services = services;
    this.preInterceptors = preInterceptors;
    this.postInterceptors = postInterceptors;
    this.antennaDoctor = antennaDoctor;
    this.antennaDoctorContext = antennaDoctorContext;
    info = new Stage.Info() {
      @Override
      public String getName() {
        return def.getName();
      }

      @Override
      public int getVersion() {
        return def.getVersion();
      }

      @Override
      public String getInstanceName() {
        return conf.getInstanceName();
      }

      @Override
      public String getLabel() {
        return label;
      }

      @Override
      public String toString() {
        return Utils.format("Info[instance='{}' name='{}' version='{}']", getInstanceName(), getName(), getVersion());
      }
    };

  }

  public Map<String, Object> getConstants() {
    return pipelineBean.getConfig().constants;
  }

  public StageDefinition getDefinition() {
    return def;
  }

  public StageConfiguration getConfiguration() {
    return conf;
  }

  public List<String> getRequiredFields() {
    return stageBean.getSystemConfigs().stageRequiredFields;
  }

  public List<String> getPreconditions() {
    return stageBean.getSystemConfigs().stageRecordPreconditions;
  }

  public OnRecordError getOnRecordError() {
    return stageBean.getSystemConfigs().stageOnRecordError;
  }

  public Stage getStage() {
    return stageBean.getStage();
  }

  public List<InterceptorRuntime> getPreInterceptors() {
    return preInterceptors;
  }

  public List<InterceptorRuntime> getPostInterceptors() {
    return postInterceptors;
  }

  public void setContext(StageContext context) {
    this.context = context;
  }

  public void setSinks(
      ErrorSink errorSink,
      EventSink eventSink,
      ProcessedSink processedSink,
      SourceResponseSink sourceResponseSink
  ) {
    context.setReportErrorDelegate(reportErrorDelegate == null ? errorSink : reportErrorDelegate);
    context.setErrorSink(errorSink);
    context.setEventSink(eventSink);
    context.setProcessedSink(processedSink);
    context.setSourceResponseSink(sourceResponseSink);
  }

  @SuppressWarnings("unchecked")
  public <T extends Stage.Context> T getContext() {
    return (T) context;
  }

  @SuppressWarnings("unchecked")
  public List<Issue> init() {
    Preconditions.checkState(context != null, "context has not been set");
    if(context.isPreview()) {
      runnerThread = Thread.currentThread().getId();
    }

    List<Issue> issues = new LinkedList<>();

    // Initialize the interceptors that are created for this stage
    for(InterceptorRuntime interceptor : Iterables.concat(preInterceptors, postInterceptors)) {
      try {
        interceptor.getContext().setAllowCreateStage(true);
        issues.addAll(interceptor.init());

        // Propagate issues from "sub-stages"
        issues.addAll(interceptor.getContext().getIssues());
      } finally {
        interceptor.getContext().setAllowCreateStage(false);
      }
    }

    // Firstly init() all services, so that Stage's init() can already use the Services if needed
    for(ServiceRuntime serviceRuntime : services) {
      issues.addAll(serviceRuntime.init());
    }

    // We initialize stage itself only if all it's services were initialized properly
    if(issues.isEmpty()) {
      issues.addAll(LambdaUtil.withClassLoader(
        getDefinition().getStageClassLoader(),
        () -> getStage().init(info, context)
      ));
    }

    return issues;
  }

  String execute(
      Callable<String> callable,
      ErrorSink errorSink,
      EventSink eventSink,
      ProcessedSink processedSink,
      SourceResponseSink sourceResponseSink
  ) throws StageException {
    mainClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      context.setPushSourceContextDelegate(this);
      setSinks(errorSink, eventSink, processedSink, sourceResponseSink);
      Thread.currentThread().setContextClassLoader(getDefinition().getStageClassLoader());

      try {
        // if the stage is annotated as recordsByRef it means it does not reuse the records/fields it creates, thus
        // we have to call it within a create-by-ref context so Field.create does not clone Fields and BatchMakerImpl
        // does not clone output records.
        return (def.getRecordsByRef() && !context.isPreview()) ? CreateByRef.call(callable) : callable.call();
      } catch (Exception ex) {
        if (ex instanceof StageException) {
          if(antennaDoctor != null) {
            StageException e = (StageException) ex;
            e.setAntennaDoctorMessages(antennaDoctor.onStage(antennaDoctorContext, e.getErrorCode(), e.getParams()));
          }
          throw (StageException) ex;
        } else if (ex instanceof RuntimeException) {
          throw (RuntimeException) ex;
        } else {
          throw new RuntimeException(ex);
        }
      }

    } finally {
      setSinks(null, null, null, null);
      Thread.currentThread().setContextClassLoader(mainClassLoader);
    }
  }

  public void execute(final Map<String, String> offsets, final int batchSize) throws StageException {
      Callable<String> callable = () -> {
        switch (getDefinition().getType()) {
          case SOURCE:
            if(getStage() instanceof PushSource) {
              ((PushSource)getStage()).produce(offsets, batchSize);
              return null;
            }
            // fall through
          default:
            throw new IllegalStateException(Utils.format("Unknown stage type: '{}'", getDefinition().getType()));
        }
      };

      execute(callable, null, null, null, null);
  }

  public String execute(
      final String previousOffset,
      final int batchSize,
      final Batch batch,
      final BatchMaker batchMaker,
      ErrorSink errorSink,
      EventSink eventSink,
      ProcessedSink processedSink,
      SourceResponseSink sourceResponseSink
  ) throws StageException {
    Callable<String> callable = () -> {
      String newOffset = null;
      switch (getDefinition().getType()) {
        case SOURCE:
          newOffset = ((Source) getStage()).produce(previousOffset, batchSize, batchMaker);
          break;
        case PROCESSOR:
          ((Processor) getStage()).process(batch, batchMaker);
          break;
        case EXECUTOR:
        case TARGET:
          ((Target) getStage()).write(batch);
          break;
        default:
          throw new IllegalStateException(Utils.format("Unknown stage type: '{}'", getDefinition().getType()));
      }
      return newOffset;
    };

    return execute(callable, errorSink, eventSink, processedSink, sourceResponseSink);
  }

  public void destroy(ErrorSink errorSink, EventSink eventSink, ProcessedSink processedSink) {
    mainClassLoader = Thread.currentThread().getContextClassLoader();

    try {
      setSinks(errorSink, eventSink, processedSink, null);

      // Firstly destroy stage itself
      LambdaUtil.withClassLoader(
        getDefinition().getStageClassLoader(),
        () -> {
          getStage().destroy();
          return null;
        }
      );

      // Then all associated services
      for (ServiceRuntime serviceRuntime : services) {
        serviceRuntime.destroy();
      }

      for(InterceptorRuntime interceptor : Iterables.concat(preInterceptors, postInterceptors)) {
        interceptor.destroy();

        for(DetachedStageRuntime stageRuntime : interceptor.getContext().getStageRuntimes()) {
          stageRuntime.runDestroy();
        }
      }
    } finally {
      // Do not eventSink and errorSink to null when in preview mode AND current thread
      // is different from the one executing stages because stages might send error to errorSink.
      if (!context.isPreview() || runnerThread == (Thread.currentThread().getId())) {
        setSinks(null, null, null,null);
      }

      // We release the stage classloader back to the library  ro reuse (as some stages my have private classloaders)
      stageBean.releaseClassLoader();
    }
  }

  public Stage.Info getInfo() {
    return info;
  }

  /**
   * For all PushSource callbacks we have to make sure that we get back to a security context
   * of SDC container module, otherwise we won't be able to update state files with new offsets
   * and other stuff.
   */

  @Override
  public final BatchContext startBatch() {
    return (BatchContext) AccessController.doPrivileged(new PrivilegedAction() {
      public Object run() {
        try {
          Thread.currentThread().setContextClassLoader(mainClassLoader);
          return pushSourceContextDelegate.startBatch();
        } finally {
          Thread.currentThread().setContextClassLoader(getDefinition().getStageClassLoader());
        }
      }
    });
  }

  @Override
  public final boolean processBatch(final BatchContext batchContext, final String entity, final String offset) {
    return (boolean) AccessController.doPrivileged(new PrivilegedAction() {
      public Object run() {
        try {
          Thread.currentThread().setContextClassLoader(mainClassLoader);
          return pushSourceContextDelegate.processBatch(batchContext, entity, offset);
        } finally {
          Thread.currentThread().setContextClassLoader(getDefinition().getStageClassLoader());
        }
      }
    });
  }

  @Override
  public final void commitOffset(final String entity, final String offset) {
    AccessController.doPrivileged(new PrivilegedAction() {
      public Object run() {
        try {
          Thread.currentThread().setContextClassLoader(mainClassLoader);
          pushSourceContextDelegate.commitOffset(entity, offset);
          return null;
        } finally {
          Thread.currentThread().setContextClassLoader(getDefinition().getStageClassLoader());
        }
      }
    });
  }

  public void setPushSourceContextDelegate(PushSourceContextDelegate delegate) {
    this.pushSourceContextDelegate = delegate;
  }

  public void setReportErrorDelegate(ReportErrorDelegate delegate) {
    this.reportErrorDelegate = delegate;
  }
}
