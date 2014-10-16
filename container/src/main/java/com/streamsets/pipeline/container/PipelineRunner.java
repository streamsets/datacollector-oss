/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.container;

import com.google.common.base.Preconditions;
import com.google.common.collect.EvictingQueue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class PipelineRunner {
  private Pipeline pipeline;
  private volatile boolean active;
  private SourceTracker sourceTracker;
  private boolean preview;
  private volatile Collection<RunOutput> storedOutputs;
  private volatile Configuration conf;

  public PipelineRunner(Pipeline pipeline, SourceTracker sourceTracker, boolean preview) {
    Preconditions.checkNotNull(pipeline, "pipeline cannot be null");
    this.pipeline = pipeline;
    this.sourceTracker = sourceTracker;
    this.preview = preview;
  }

  public void run() {
    Preconditions.checkState(!preview, "Preview mode, cannot run");
    boolean sourceFinished = false;
    active = true;
    while (active && !sourceFinished) {
      Collection<RunOutput> storedOutputsLocal = storedOutputs;
      PipelineBatch batch = (storedOutputsLocal == null) ? new PipelineBatch(sourceTracker.getLastBatchId())
                                                         : new PipelineBatchRunOutput(sourceTracker.getLastBatchId());
      if (conf != null) {
        pipeline.configure(conf);
        conf = null;
      }
      pipeline.runBatch(batch);
      sourceTracker.udpateLastBatchId(batch.getBatchId());
      if (storedOutputsLocal != null) {
        storedOutputsLocal.add((RunOutput) batch);
      }
      if (batch.getBatchId() == null) {
        sourceFinished = true;
      }
    }
  }

  @SuppressWarnings("unchecked")
  public void setKeepOutputs(int numberOfOutputs) {
    if (numberOfOutputs > 0) {
      storedOutputs = (Collection<RunOutput>) (Collection)
          Collections.synchronizedCollection(EvictingQueue.create(numberOfOutputs));
    } else {
      storedOutputs = null;
    }
  }

  @SuppressWarnings({"unchecked", "SynchronizationOnLocalVariableOrMethodParameter"})
  public List<RunOutput> getStoredRunOutputs() {
    Collection<RunOutput> storedOutputsLocal = storedOutputs;
    if (storedOutputsLocal != null) {
      // the queue is synchronized, but iterator operations are not guarded by the lock,
      // so we have to manually synchronize
      synchronized (storedOutputsLocal) {
        return new ArrayList<RunOutput>(storedOutputsLocal);
      }
    } else {
      return Collections.EMPTY_LIST;
    }
  }

  public RunOutput step() {
    Preconditions.checkState(!preview, "Preview mode, cannot step");
    Preconditions.checkState(!sourceTracker.isFinished(), "Pipeline source does not have more data");
    PipelineBatchRunOutput batch = new PipelineBatchRunOutput(sourceTracker.getLastBatchId());
    if (conf != null) {
      pipeline.configure(conf);
      conf = null;
    }
    pipeline.runBatch(batch);
    sourceTracker.udpateLastBatchId(batch.getBatchId());
    Collection<RunOutput> storedOutputsLocal = storedOutputs;
    if (storedOutputsLocal != null) {
      storedOutputsLocal.add(batch);
    }
    return batch;
  }

  public void stop() {
    active = false;
  }

  public boolean hasFinished() {
    return sourceTracker.getLastBatchId() == null;
  }

  public void reConfigure(Configuration conf) {
    Preconditions.checkNotNull(conf, "conf cannot be null");
    this.conf = conf;
  }

  public RunOutput preview(String batchId) {
    Preconditions.checkState(preview, "Run mode, cannot preview");
    // if preview exhausts the source, we loop back to the current starting point
    batchId = (batchId == null) ? sourceTracker.getLastBatchId() : batchId;
    PipelineBatchRunOutput batch = new PipelineBatchRunOutput(batchId);
    pipeline.runBatch(batch);
    return batch;
  }
}
