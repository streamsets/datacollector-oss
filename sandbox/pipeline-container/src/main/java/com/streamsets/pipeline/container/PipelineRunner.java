/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.container;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.config.Configuration;

public class PipelineRunner {
  private Pipeline pipeline;
  private volatile boolean active;
  private SourceTracker sourceTracker;
  private boolean preview;
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
    while (active || sourceFinished) {
      PipelineBatch batch = new PipelineBatch(sourceTracker.getLastBatchId());
      if (conf != null) {
        pipeline.configure(conf);
        conf = null;
      }
      pipeline.runBatch(batch);
      sourceTracker.udpateLastBatchId(batch.getBatchId());
      if (batch.getBatchId() == null) {
        sourceFinished = true;
      }
    }
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

  public PreviewOutput preview(String batchId) {
    Preconditions.checkState(!preview, "Run mode, cannot preview");
    // if preview exhausts the source, we loop back to the current starting point
    batchId = (batchId == null) ? sourceTracker.getLastBatchId() : batchId;
    PreviewPipelineBatch batch = new PreviewPipelineBatch(batchId);
    pipeline.runBatch(batch);
    return batch;
  }
}
