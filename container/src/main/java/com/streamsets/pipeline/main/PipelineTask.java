/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.main;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.http.WebServerTask;
import com.streamsets.pipeline.prodmanager.PipelineManager;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.task.CompositeTask;

import javax.inject.Inject;

public class PipelineTask extends CompositeTask {

  private final PipelineManager productionPipelineManagerTask;
  private final PipelineStoreTask pipelineStoreTask;
  private final StageLibraryTask stageLibraryTask;

  @Inject
  public PipelineTask(StageLibraryTask library, PipelineStoreTask store, PipelineManager pipelineManager,
      WebServerTask webServer) {
    super("pipelineNode", ImmutableList.of(library, store, pipelineManager, webServer),
      true);
    this.stageLibraryTask = library;
    this.pipelineStoreTask = store;
    this.productionPipelineManagerTask = pipelineManager;
  }

  public PipelineManager getProductionPipelineManagerTask() {
    return productionPipelineManagerTask;
  }
  public PipelineStoreTask getPipelineStoreTask() {
    return pipelineStoreTask;
  }
  public StageLibraryTask getStageLibraryTask() {
    return stageLibraryTask;
  }
}
