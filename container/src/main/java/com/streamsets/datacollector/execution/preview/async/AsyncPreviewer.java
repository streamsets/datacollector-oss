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
package com.streamsets.datacollector.execution.preview.async;

import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.execution.PreviewOutput;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.RawPreview;
import com.streamsets.datacollector.execution.preview.common.PreviewError;
import com.streamsets.datacollector.execution.preview.sync.SyncPreviewer;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;

import javax.inject.Inject;
import javax.ws.rs.core.MultivaluedMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AsyncPreviewer implements Previewer {

  private final SyncPreviewer syncPreviewer;
  private final SafeScheduledExecutorService executorService;
  private Future<?> future;

  @Inject
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
  public Map<String, ConnectionConfiguration> getConnections() {
    return syncPreviewer.getConnections();
  }

  @Override
  public List<PipelineStartEvent.InterceptorConfiguration> getInterceptorConfs() {
    return syncPreviewer.getInterceptorConfs();
  }

  @Override
  public void validateConfigs(final long timeoutMillis) {
    Callable<Object> callable = () -> {
      syncPreviewer.validateConfigs(timeoutMillis);
      return null;
    };
    future = executorService.submit(callable);
    scheduleTimeout(timeoutMillis);
  }

  @Override
  public RawPreview getRawSource(int maxLength, MultivaluedMap<String, String> previewParams) throws PipelineException {
    return syncPreviewer.getRawSource(maxLength, previewParams);
  }

  @Override
  public void start(
      final int batches,
      final int batchSize,
      final boolean skipTargets,
      final boolean skipLifecycleEvents,
      final String stopStage,
      final List<StageOutput> stagesOverride,
      final long timeoutMillis,
      final boolean testOrigin
  ) {
    Callable<Object> callable = () -> {
      syncPreviewer.start(
          batches,
          batchSize,
          skipTargets,
          skipLifecycleEvents,
          stopStage,
          stagesOverride,
          timeoutMillis,
          testOrigin
      );
      return null;
    };
    future = executorService.submit(callable);
    scheduleTimeout(timeoutMillis);
  }

  @Override
  public void stop() {
    if (future != null) {
      synchronized (future) {
        if(!future.isDone()) {
          syncPreviewer.prepareForTimeout();
          future.cancel(true);
          syncPreviewer.stop();
        } else {
          syncPreviewer.runAfterActionsIfNecessary();
        }
      }
    }
  }

  @Override
  public boolean waitForCompletion(long timeoutMillis) throws PipelineException {
    if(future == null) {
      throw new PipelineRuntimeException(PreviewError.PREVIEW_0001);
    }
    try {
      future.get(timeoutMillis, TimeUnit.MILLISECONDS);
      return true;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof PipelineException) {
        //preview error from pipeline
        throw (PipelineException)e.getCause();
      } else {
        //some exception while previewing
        throw new PipelineException(PreviewError.PREVIEW_0003, e.toString(), e);
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
    return (future.isDone() || syncPreviewer.getOutput() != null) ? syncPreviewer.getOutput() : null;
  }

  private void scheduleTimeout(long timeoutMillis) {
    executorService.schedule(new Callable<Object>() {
      @Override
      public Object call() throws PipelineException {
        if (future != null) {
          synchronized (future) {
            if (!future.isDone()) {
              syncPreviewer.prepareForTimeout();
              future.cancel(true);
              syncPreviewer.timeout();
              return true;
            }
          }
        }
        return false;
      }
    }, timeoutMillis, TimeUnit.MILLISECONDS);
  }

}
