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
package com.streamsets.datacollector.execution.preview.sync;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.blobstore.BlobStoreTask;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RawSourceDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.el.JobEL;
import com.streamsets.datacollector.el.PipelineEL;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.execution.PreviewOutput;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.PreviewerListener;
import com.streamsets.datacollector.execution.RawPreview;
import com.streamsets.datacollector.execution.preview.common.PreviewError;
import com.streamsets.datacollector.execution.preview.common.PreviewOutputImpl;
import com.streamsets.datacollector.execution.preview.common.RawPreviewImpl;
import com.streamsets.datacollector.execution.runner.common.PipelineStopReason;
import com.streamsets.datacollector.lineage.LineagePublisherTask;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.PreviewResource;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.SourceOffsetTracker;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.runner.UserContext;
import com.streamsets.datacollector.runner.preview.PreviewPipeline;
import com.streamsets.datacollector.runner.preview.PreviewPipelineBuilder;
import com.streamsets.datacollector.runner.preview.PreviewPipelineOutput;
import com.streamsets.datacollector.runner.preview.PreviewPipelineRunner;
import com.streamsets.datacollector.runner.preview.PreviewSourceOffsetTracker;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.usagestats.StatsCollector;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.pipeline.api.RawSourcePreviewer;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageType;
import dagger.ObjectGraph;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class SyncPreviewer implements Previewer {
  private static final Logger LOG = LoggerFactory.getLogger(SyncPreviewer.class);

  private static final String MAX_SOURCE_PREVIEW_SIZE_KEY = "preview.maxSourcePreviewSize";
  private static final int MAX_SOURCE_PREVIEW_SIZE_DEFAULT = 4*1024;

  private final String id;
  private final UserContext userContext;
  private final String name;
  private final String rev;
  private final Map<String, ConnectionConfiguration> connections;
  private final PreviewerListener previewerListener;
  private final List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs;
  private final Function afterActionsFunction;

  @Inject Configuration configuration;
  @Inject StageLibraryTask stageLibrary;
  @Inject PipelineStoreTask pipelineStore;
  @Inject RuntimeInfo runtimeInfo;
  @Inject BuildInfo buildInfo;
  @Inject BlobStoreTask blobStoreTask;
  @Inject LineagePublisherTask lineagePublisherTask;
  @Inject StatsCollector statsCollector;
  private volatile PreviewStatus previewStatus;
  private volatile PreviewOutput previewOutput;
  private volatile PreviewPipeline previewPipeline;
  private volatile boolean timingOut = false;

  public SyncPreviewer(
      String id,
      String user,
      String name,
      String rev,
      PreviewerListener previewerListener,
      ObjectGraph objectGraph,
      List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs,
      Function<Object, Void> afterActionsFunction,
      Map<String, ConnectionConfiguration> connections
  ) {
    objectGraph.inject(this);
    this.id = id;
    boolean aliasNameEnabled = this.configuration.get(RemoteSSOService.DPM_USER_ALIAS_NAME_ENABLED,
        RemoteSSOService.DPM_USER_ALIAS_NAME_ENABLED_DEFAULT);
    this.userContext = new UserContext(
        user,
        runtimeInfo.isDPMEnabled(),
        aliasNameEnabled
    );
    this.name = name;
    this.rev = rev;
    this.previewerListener = previewerListener;
    this.previewStatus = PreviewStatus.CREATED;
    this.interceptorConfs = interceptorConfs;
    this.afterActionsFunction = afterActionsFunction;
    this.connections = connections;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getRev() {
    return rev;
  }

  @Override
  public List<PipelineStartEvent.InterceptorConfiguration> getInterceptorConfs() {
    return interceptorConfs;
  }

  @Override
  public Map<String, ConnectionConfiguration> getConnections() {
    return connections;
  }

  @Override
  public void validateConfigs(long timeoutMillis) throws PipelineException {
    changeState(PreviewStatus.VALIDATING, null);
    try {
      previewPipeline = buildPreviewPipeline(0, 0, null, false, true, false);
      List<Issue> stageIssues = previewPipeline.validateConfigs();
      PreviewStatus status = stageIssues.size() == 0 ? PreviewStatus.VALID : PreviewStatus.INVALID;
      changeState(status, new PreviewOutputImpl(status, new Issues(stageIssues), (List)null));
    } catch (PipelineRuntimeException e) {
      //Preview Pipeline Builder validates configurations and throws PipelineRuntimeException with code CONTAINER_0165
      //for validation errors.
      if (e.getErrorCode() == ContainerError.CONTAINER_0165) {
        changeState(PreviewStatus.INVALID, new PreviewOutputImpl(PreviewStatus.INVALID, e.getIssues(), e));
      } else {
        changeState(PreviewStatus.VALIDATION_ERROR, new PreviewOutputImpl(PreviewStatus.VALIDATION_ERROR, e));
        throw e;
      }
    } catch (PipelineStoreException e) {
      changeState(PreviewStatus.VALIDATION_ERROR, new PreviewOutputImpl(PreviewStatus.VALIDATION_ERROR, e));
      throw e;
    } catch (Throwable e) {
      //Wrap stage exception in PipelineException
      changeState(PreviewStatus.VALIDATION_ERROR, new PreviewOutputImpl(PreviewStatus.VALIDATION_ERROR, e));
      throw new PipelineException(PreviewError.PREVIEW_0003, e.toString(), e) ;
    } finally {
      PipelineEL.unsetConstantsInContext();
      JobEL.unsetConstantsInContext();
    }
  }

  @Override
  public RawPreview getRawSource(int maxLength, MultivaluedMap<String, String> previewParams) throws PipelineException {
    changeState(PreviewStatus.RUNNING, null);
    int bytesToRead = configuration.get(MAX_SOURCE_PREVIEW_SIZE_KEY, MAX_SOURCE_PREVIEW_SIZE_DEFAULT);
    bytesToRead = Math.min(bytesToRead, maxLength);

    PipelineConfiguration pipelineConf = pipelineStore.load(name, rev);
    if(pipelineConf.getStages().isEmpty()) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0159, name);
    }

    //find the source stage in the pipeline configuration
    StageDefinition sourceStageDef = getSourceStageDef(pipelineConf);

    RawSourcePreviewer rawSourcePreviewer = createRawSourcePreviewer(sourceStageDef, previewParams);
    RawPreview rawPreview;
    ClassLoader classLoader = sourceStageDef.getStageClassLoader();
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(classLoader);
      try(BoundedInputStream bIn = new BoundedInputStream(rawSourcePreviewer.preview(bytesToRead), bytesToRead)) {
        rawPreview = new RawPreviewImpl(IOUtils.toString(bIn), rawSourcePreviewer.getMimeType());
      }
    } catch (IOException ex) {
      throw new PipelineRuntimeException(PreviewError.PREVIEW_0003, ex.toString(), ex);
    } finally {
      Thread.currentThread().setContextClassLoader(contextClassLoader);
    }
    changeState(PreviewStatus.FINISHED, null);
    return rawPreview;
  }

  @Override
  public void start(
      int batches,
      int batchSize,
      boolean skipTargets,
      boolean skipLifecycleEvents,
      String stopStage,
      List<StageOutput> stagesOverride,
      long timeoutMillis,
      boolean testOrigin
  ) throws PipelineException {
    changeState(PreviewStatus.RUNNING, null);
    try {
      previewPipeline = buildPreviewPipeline(batches, batchSize, stopStage, skipTargets, skipLifecycleEvents, testOrigin);
      PreviewPipelineOutput output = previewPipeline.run(stagesOverride);
      changeState(PreviewStatus.FINISHED, new PreviewOutputImpl(PreviewStatus.FINISHED, output.getIssues(),
          output.getBatchesOutput()));
    } catch (PipelineRuntimeException e) {
      if(timingOut) {
        LOG.debug("Ignoring exception during time out {}", e.toString(), e);
        return;
      }
      //Preview Pipeline Builder validates configurations and throws PipelineRuntimeException with code CONTAINER_0165
      //for validation errors.
      if (e.getErrorCode() == ContainerError.CONTAINER_0165) {
        changeState(PreviewStatus.INVALID, new PreviewOutputImpl(PreviewStatus.INVALID, e.getIssues(), e));
      } else {
        changeState(PreviewStatus.RUN_ERROR, new PreviewOutputImpl(PreviewStatus.RUN_ERROR, e.getIssues(), e));
        throw e;
      }
    } catch (PipelineStoreException e) {
      if(timingOut) {
        LOG.debug("Ignoring exception during time out {}", e.toString(), e);
        return;
      }
      changeState(PreviewStatus.RUN_ERROR, new PreviewOutputImpl(PreviewStatus.RUN_ERROR, e));
      throw e;
    } catch (Throwable e) {
      if(timingOut) {
        LOG.debug("Ignoring exception during time out {}", e.toString(), e);
        return;
      }
      changeState(PreviewStatus.RUN_ERROR, new PreviewOutputImpl(PreviewStatus.RUN_ERROR, e));
      throw new PipelineException(PreviewError.PREVIEW_0003, e.toString(), e);
    } finally {
      if (previewPipeline != null) {
        try {
          previewPipeline.destroy(PipelineStopReason.FINISHED);
        } catch (StageException e) {
          throw new PipelineException(PreviewError.PREVIEW_0003, e.toString(), e);
        }
        previewPipeline = null;
      }
      PipelineEL.unsetConstantsInContext();
      JobEL.unsetConstantsInContext();
    }
  }

  @Override
  public void stop() {
    //state is active then call cancelling otherwise just destroy
    if(previewStatus.isActive()) {
      changeState(PreviewStatus.CANCELLING, null);
    }
    destroyPipeline(PipelineStopReason.USER_ACTION);
    if(previewStatus == PreviewStatus.CANCELLING) {
      changeState(PreviewStatus.CANCELLED, null);
    }
    PipelineEL.unsetConstantsInContext();
    JobEL.unsetConstantsInContext();
    runAfterActionsIfNecessary();
  }

  public void runAfterActionsIfNecessary() {
    if (afterActionsFunction != null) {
      // to make ErrorProne happy
      final Object ignored = afterActionsFunction.apply(this);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Return value from afterActionsFunction: {}", ignored);
      }
    }
  }

  public void prepareForTimeout() {
    this.timingOut = true;
  }

  public void timeout() {
    //state is active then call cancelling otherwise just destroy
    if(previewStatus.isActive()) {
      changeState(PreviewStatus.TIMING_OUT, null);
    }
    destroyPipeline(PipelineStopReason.FAILURE);
    if(previewStatus == PreviewStatus.TIMING_OUT) {
      changeState(PreviewStatus.TIMED_OUT, null);
    }
    PipelineEL.unsetConstantsInContext();
    JobEL.unsetConstantsInContext();
  }

  private void destroyPipeline(PipelineStopReason reason) {
    if(previewPipeline == null) {
      return;
    }

    try {
      previewPipeline.destroy(reason);
    } catch (StageException|PipelineRuntimeException e) {
      LOG.error("Error destroying pipeline", e);
    }
    previewPipeline = null;
  }


  @Override
  public boolean waitForCompletion(long timeoutMillis) {
    return true;
  }

  @Override
  public PreviewStatus getStatus() {
    return previewStatus;
  }

  @Override
  public PreviewOutput getOutput() {
    //return output only if the preview has finished or terminated
    if(!previewStatus.isActive()) {
      previewerListener.outputRetrieved(id);
      return previewOutput;
    }
    return null;
  }

  @VisibleForTesting
  PreviewPipeline buildPreviewPipeline(
      int batches,
      int batchSize,
      String endStageInstanceName,
      boolean skipTargets,
      boolean skipLifecycleEvents,
      boolean testOrigin
  ) throws PipelineException {
    int maxBatchSize = configuration.get(PreviewResource.MAX_BATCH_SIZE_KEY, PreviewResource.MAX_BATCH_SIZE_DEFAULT);
    batchSize = Math.min(maxBatchSize, batchSize);
    int maxBatches = configuration.get(PreviewResource.MAX_BATCHES_KEY, PreviewResource.MAX_BATCHES_DEFAULT);
    PipelineConfiguration pipelineConf = pipelineStore.load(name, rev);
    PipelineEL.setConstantsInContext(
        pipelineConf,
        userContext,
        System.currentTimeMillis()
    );
    JobEL.setConstantsInContext(null);
    batches = Math.min(maxBatches, batches);
    SourceOffsetTracker tracker = new PreviewSourceOffsetTracker(Collections.emptyMap());
    PreviewPipelineRunner runner = new PreviewPipelineRunner(
        name,
        rev,
        buildInfo,
        runtimeInfo,
        tracker,
        batchSize,
        batches,
        skipTargets,
        skipLifecycleEvents,
        testOrigin
    );
    return new PreviewPipelineBuilder(
        stageLibrary,
        buildInfo,
        configuration,
        runtimeInfo,
        name,
        rev,
        pipelineConf,
        endStageInstanceName,
        blobStoreTask,
        lineagePublisherTask,
        statsCollector,
        testOrigin,
        interceptorConfs,
        connections
    ).build(userContext, runner);
  }

  private RawSourcePreviewer createRawSourcePreviewer(
      StageDefinition sourceStageDef,
      MultivaluedMap<String, String> previewParams
  ) throws PipelineRuntimeException {

    RawSourceDefinition rawSourceDefinition = sourceStageDef.getRawSourceDefinition();
    List<ConfigDefinition> configDefinitions = rawSourceDefinition.getConfigDefinitions();

    validateParameters(previewParams, configDefinitions);

    //Attempt to load the previewer class from stage class loader
    Class previewerClass;
    try {
      previewerClass = sourceStageDef.getStageClassLoader().loadClass(sourceStageDef.getRawSourceDefinition()
          .getRawSourcePreviewerClass());
    } catch (ClassNotFoundException e) {
      //Try loading from this class loader
      try {
        previewerClass = getClass().getClassLoader().loadClass(
            sourceStageDef.getRawSourceDefinition().getRawSourcePreviewerClass());
      } catch (ClassNotFoundException e1) {
        throw new RuntimeException(e1);
      }
    }

    RawSourcePreviewer rawSourcePreviewer;
    try {
      rawSourcePreviewer = (RawSourcePreviewer) previewerClass.newInstance();
      //inject values from url to fields in the rawSourcePreviewer
      for(ConfigDefinition confDef : configDefinitions) {
        Field f = previewerClass.getField(confDef.getFieldName());
        f.set(rawSourcePreviewer, getValueFromParam(f, previewParams.get(confDef.getName()).get(0)));
      }
      rawSourcePreviewer.setMimeType(rawSourceDefinition.getMimeType());
    } catch (IllegalAccessException | InstantiationException | NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
    return rawSourcePreviewer;
  }

  private static Object getValueFromParam(Field field, String stringValue) {
    Class<?> type = field.getType();
    if(String.class.isAssignableFrom(type)) {
      return stringValue;
    } else if (Integer.class.isAssignableFrom(type) || Integer.TYPE == type) {
      return Integer.parseInt(stringValue);
    } else if (Long.class.isAssignableFrom(type) || Long.TYPE == type) {
      return Long.parseLong(stringValue);
    } else if (Boolean.class.isAssignableFrom(type) || Boolean.TYPE == type) {
      return Boolean.parseBoolean(stringValue);
    }
    return null;
  }

  private static void validateParameters(
      MultivaluedMap<String, String> previewParams,
      List<ConfigDefinition> configDefinitions
  ) throws PipelineRuntimeException {
    //validate that all configuration required by config definitions are supplied through the URL
    List<String> requiredPropertiesNotSet = new ArrayList<>();
    for(ConfigDefinition confDef: configDefinitions) {
      if(confDef.isRequired() && !previewParams.containsKey(confDef.getName())) {
        requiredPropertiesNotSet.add(confDef.getName());
      }
    }

    if(!requiredPropertiesNotSet.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append(requiredPropertiesNotSet.get(0));
      for(int i = 1; i < requiredPropertiesNotSet.size(); i++) {
        sb.append(", ").append(requiredPropertiesNotSet.get(i));
      }
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0160, sb.toString());
    }
  }

  private void changeState(PreviewStatus previewStatus, PreviewOutput previewOutput) {
    this.previewStatus = previewStatus;
    this.previewOutput = previewOutput;
    this.previewerListener.statusChange(id, previewStatus);
  }

  private StageDefinition getSourceStageDef(PipelineConfiguration pipelineConf) {
    StageDefinition sourceStageDef = null;
    for(StageConfiguration stageConf : pipelineConf.getStages()) {
      StageDefinition stageDefinition = stageLibrary.getStage(stageConf.getLibrary(), stageConf.getStageName(),
          false);
      if(stageDefinition.getType() == StageType.SOURCE) {
        sourceStageDef = stageDefinition;
      }
    }
    return sourceStageDef;
  }
}
