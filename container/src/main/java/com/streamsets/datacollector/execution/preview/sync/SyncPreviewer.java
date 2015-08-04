/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.preview.sync;

import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RawSourceDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.execution.PreviewOutput;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.PreviewerListener;
import com.streamsets.datacollector.execution.RawPreview;
import com.streamsets.datacollector.execution.preview.common.PreviewError;
import com.streamsets.datacollector.execution.preview.common.PreviewOutputImpl;
import com.streamsets.datacollector.execution.preview.common.RawPreviewImpl;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.SourceOffsetTracker;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.runner.preview.PreviewPipeline;
import com.streamsets.datacollector.runner.preview.PreviewPipelineBuilder;
import com.streamsets.datacollector.runner.preview.PreviewPipelineOutput;
import com.streamsets.datacollector.runner.preview.PreviewPipelineRunner;
import com.streamsets.datacollector.runner.preview.PreviewSourceOffsetTracker;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.pipeline.api.RawSourcePreviewer;
import com.streamsets.pipeline.api.StageException;

import dagger.ObjectGraph;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;

import javax.inject.Inject;
import javax.ws.rs.core.MultivaluedMap;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class SyncPreviewer implements Previewer {

  private static final String MAX_BATCH_SIZE_KEY = "preview.maxBatchSize";
  private static final int MAX_BATCH_SIZE_DEFAULT = 10;
  private static final String MAX_BATCHES_KEY = "preview.maxBatches";
  private static final int MAX_BATCHES_DEFAULT = 10;
  private static final String MAX_SOURCE_PREVIEW_SIZE_KEY = "preview.maxSourcePreviewSize";
  private static final int MAX_SOURCE_PREVIEW_SIZE_DEFAULT = 4*1024;

  private final String id;
  private final String name;
  private final String rev;
  private final PreviewerListener previewerListener;
  @Inject Configuration configuration;
  @Inject StageLibraryTask stageLibrary;
  @Inject PipelineStoreTask pipelineStore;
  @Inject RuntimeInfo runtimeInfo;
  private volatile PreviewStatus previewStatus;
  private volatile PreviewOutput previewOutput;
  private volatile PreviewPipeline previewPipeline;

  public SyncPreviewer(String id, String name, String rev, PreviewerListener previewerListener,
                       ObjectGraph objectGraph) {
    this.id = id;
    this.name = name;
    this.rev = rev;
    this.previewerListener = previewerListener;
    objectGraph.inject(this);
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
  public void validateConfigs(long timeoutMillis) throws PipelineException {
    changeState(PreviewStatus.VALIDATING, null);
    try {
      previewPipeline = buildPreviewPipeline(0, 0, null, false);
      List<Issue> stageIssues = previewPipeline.validateConfigs();
      PreviewStatus status = stageIssues.size() == 0 ? PreviewStatus.VALID : PreviewStatus.INVALID;
      changeState(status, new PreviewOutputImpl(status, new Issues(stageIssues), null, null));
    } catch (PipelineRuntimeException e) {
      //Preview Pipeline Builder validates configurations and throws PipelineRuntimeException with code CONTAINER_0165
      //for validation errors.
      if (e.getErrorCode() == ContainerError.CONTAINER_0165) {
        changeState(PreviewStatus.INVALID, new PreviewOutputImpl(PreviewStatus.INVALID, e.getIssues(), null,
          e.toString()));
      } else {
        //Leave the state as is.
        throw e;
      }
    } catch (PipelineStoreException e) {
      //Leave the state as is.
      throw e;
    } catch (StageException e) {
      //Wrap stage exception in PipelineException
      throw new PipelineException(PreviewError.PREVIEW_0003, e.toString(), e) ;
    }  finally {
      if(previewPipeline != null) {
        previewPipeline.destroy();
        previewPipeline = null;
      }
    }
  }

  @Override
  public RawPreview getRawSource(int maxLength, MultivaluedMap<String, String> previewParams)
    throws PipelineRuntimeException, PipelineStoreException {
    changeState(PreviewStatus.RUNNING, null);
    int bytesToRead = configuration.get(MAX_SOURCE_PREVIEW_SIZE_KEY, MAX_SOURCE_PREVIEW_SIZE_DEFAULT);
    bytesToRead = Math.min(bytesToRead, maxLength);
    RawSourcePreviewer rawSourcePreviewer = createRawSourcePreviewer(previewParams);
    RawPreview rawPreview;
    try(BoundedInputStream bIn = new BoundedInputStream(rawSourcePreviewer.preview(bytesToRead), bytesToRead)) {
      rawPreview = new RawPreviewImpl(IOUtils.toString(bIn), rawSourcePreviewer.getMimeType());
    } catch (IOException ex) {
      throw new PipelineRuntimeException(PreviewError.PREVIEW_0003, ex.toString(), ex);
    }
    changeState(PreviewStatus.FINISHED, null);
    return rawPreview;
  }

  @Override
  public void start(int batches, int batchSize, boolean skipTargets, String stopStage, List<StageOutput> stagesOverride,
                    long timeoutMillis)
    throws PipelineException {
    changeState(PreviewStatus.RUNNING, null);
    try {
      previewPipeline = buildPreviewPipeline(batches, batchSize, stopStage, skipTargets);
      PreviewPipelineOutput output = previewPipeline.run(stagesOverride);
      changeState(PreviewStatus.FINISHED, new PreviewOutputImpl(PreviewStatus.FINISHED, output.getIssues(),
        output.getBatchesOutput(), null));
    } catch (PipelineRuntimeException e) {
      //Preview Pipeline Builder validates configurations and throws PipelineRuntimeException with code CONTAINER_0165
      //for validation errors.
      if (e.getErrorCode() == ContainerError.CONTAINER_0165) {
        changeState(PreviewStatus.INVALID, new PreviewOutputImpl(PreviewStatus.INVALID, e.getIssues(), null,
          e.toString()));
      } else {
        changeState(PreviewStatus.RUN_ERROR, new PreviewOutputImpl(PreviewStatus.RUN_ERROR, e.getIssues(), null,
          e.toString()));
        throw e;
      }
    } catch (PipelineStoreException e) {
      changeState(PreviewStatus.RUN_ERROR, new PreviewOutputImpl(PreviewStatus.RUN_ERROR, null, null, e.toString()));
      throw e;
    } catch (Exception e) {
      changeState(PreviewStatus.RUN_ERROR, new PreviewOutputImpl(PreviewStatus.RUN_ERROR, null, null, e.toString()));
      throw new PipelineException(PreviewError.PREVIEW_0003, e.toString(), e);
    } finally {
      if(previewPipeline != null) {
        previewPipeline.destroy();
        previewPipeline = null;
      }
    }
  }

  @Override
  public void stop() {
    //state is active then call cancelling otherwise just destroy
    if(previewStatus.isActive()) {
      changeState(PreviewStatus.CANCELLING, null);
    }
    if(previewPipeline != null) {
      previewPipeline.destroy();
      previewPipeline = null;
    }
    if(previewStatus == PreviewStatus.CANCELLING) {
      changeState(PreviewStatus.CANCELLED, null);
    }
  }

  public void timeout() {
    //state is active then call cancelling otherwise just destroy
    if(previewStatus.isActive()) {
      changeState(PreviewStatus.TIMING_OUT, null);
    }
    if(previewPipeline != null) {
      previewPipeline.destroy();
      previewPipeline = null;
    }
    if(previewStatus == PreviewStatus.TIMING_OUT) {
      changeState(PreviewStatus.TIMED_OUT, null);
    }
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

  private PreviewPipeline buildPreviewPipeline(int batches, int batchSize, String endStageInstanceName,
                                               boolean skipTargets)
    throws PipelineStoreException, StageException, PipelineRuntimeException {

    int maxBatchSize = configuration.get(MAX_BATCH_SIZE_KEY, MAX_BATCH_SIZE_DEFAULT);
    batchSize = Math.min(maxBatchSize, batchSize);
    int maxBatches = configuration.get(MAX_BATCHES_KEY, MAX_BATCHES_DEFAULT);
    PipelineConfiguration pipelineConf = pipelineStore.load(name, rev);
    batches = Math.min(maxBatches, batches);
    SourceOffsetTracker tracker = new PreviewSourceOffsetTracker(null);
    PreviewPipelineRunner runner = new PreviewPipelineRunner(name, rev, runtimeInfo, tracker, batchSize, batches,
      skipTargets);
    return new PreviewPipelineBuilder(stageLibrary, name, rev, pipelineConf, endStageInstanceName)
      .build(runner);
  }

  private RawSourcePreviewer createRawSourcePreviewer(MultivaluedMap<String, String> previewParams)
    throws PipelineRuntimeException, PipelineStoreException {

    PipelineConfiguration pipelineConf = pipelineStore.load(name, rev);
    if(pipelineConf.getStages().isEmpty()) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0159, name);
    }

    //find the source stage in the pipeline configuration
    StageDefinition sourceStageDef = null;
    for(StageConfiguration stageConf : pipelineConf.getStages()) {
      StageDefinition stageDefinition = stageLibrary.getStage(stageConf.getLibrary(), stageConf.getStageName(),
                                                              false);
      if(stageDefinition.getType() == StageType.SOURCE) {
        sourceStageDef = stageDefinition;
      }
    }
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

  private static void validateParameters(MultivaluedMap<String, String> previewParams,
                                         List<ConfigDefinition> configDefinitions) throws PipelineRuntimeException {
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
}
