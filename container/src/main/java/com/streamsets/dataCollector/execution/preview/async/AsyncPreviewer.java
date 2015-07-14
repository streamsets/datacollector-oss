/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.preview.async;

import com.streamsets.dataCollector.execution.PreviewOutput;
import com.streamsets.dataCollector.execution.PreviewStatus;
import com.streamsets.dataCollector.execution.Previewer;
import com.streamsets.dataCollector.execution.RawPreview;
import com.streamsets.dataCollector.execution.preview.common.PreviewError;
import com.streamsets.dataCollector.execution.preview.sync.SyncPreviewer;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.StageOutput;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.util.PipelineException;

import javax.ws.rs.core.MultivaluedMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AsyncPreviewer implements Previewer {

  private final SyncPreviewer syncPreviewer;
  private final SafeScheduledExecutorService executorService;
  private Future<?> future;

  public AsyncPreviewer(SyncPreviewer syncPreviewer, SafeScheduledExecutorService executorService) {
    this.syncPreviewer = syncPreviewer;
    this.executorService = executorService;
  }

  @Override
  public String getId() {
    return syncPreviewer.getId();
  }

  @Override
  public String getName() {
    return syncPreviewer.getName();
  }

  @Override
  public String getRev() {
    return syncPreviewer.getRev();
  }

  @Override
  public void validateConfigs() throws PipelineException {
    Callable<Object> callable = new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        syncPreviewer.validateConfigs();
        return null;
      }
    };
    future = executorService.submit(callable);
  }

  @Override
  public RawPreview getRawSource(int maxLength, MultivaluedMap<String, String> previewParams)
    throws PipelineRuntimeException, PipelineStoreException {
    return syncPreviewer.getRawSource(maxLength, previewParams);
  }

  @Override
  public void start(final int batches, final int batchSize, final boolean skipTargets, final String stopStage,
                    final List<StageOutput> stagesOverride) {
    Callable<Object> callable = new Callable<Object>() {
      @Override
      public Object call() throws PipelineException {
        syncPreviewer.start(batches, batchSize, skipTargets, stopStage, stagesOverride);
        return null;
      }
    };
    future = executorService.submit(callable);
  }

  @Override
  public void stop() {
    if(future != null && !future.isDone()) {
      future.cancel(true);
      syncPreviewer.stop();
    }
  }

  @Override
  public boolean waitForCompletion(int millis) throws PipelineException {
    if(future == null) {
      throw new PipelineRuntimeException(PreviewError.PREVIEW_0001);
    }
    try {
      future.get(millis, TimeUnit.MILLISECONDS);
      return true;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof PipelineException) {
        //preview error from pipeline
        throw (PipelineException)e.getCause();
      } else {
        //some exception while previewing
        throw new PipelineException(PreviewError.PREVIEW_0003, e.getMessage(), e);
      }
    } catch (InterruptedException | TimeoutException e) {
      return false;
    }
  }

  @Override
  public PreviewStatus getStatus() {
    return syncPreviewer.getStatus();
  }

  @Override
  public PreviewOutput getOutput() {
    return (future.isDone()) ? syncPreviewer.getOutput() : null;
  }

}
