/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.main;

import com.google.common.collect.ImmutableList;
import com.streamsets.dataCollector.execution.Manager;
import com.streamsets.pipeline.http.WebServerTask;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.task.CompositeTask;

import javax.inject.Inject;

public class PipelineTask extends CompositeTask {

  private final Manager manager;
  private final PipelineStoreTask pipelineStoreTask;
  private final StageLibraryTask stageLibraryTask;
  private final WebServerTask webServerTask;

  @Inject
  public PipelineTask(StageLibraryTask library, PipelineStoreTask store, Manager manager,
      WebServerTask webServerTask) {
    super("pipelineNode", ImmutableList.of(library, store, manager, webServerTask),
      true);
    this.webServerTask = webServerTask;
    this.stageLibraryTask = library;
    this.pipelineStoreTask = store;
    this.manager = manager;
  }

  public Manager getManager() {
    return manager;
  }
  public PipelineStoreTask getPipelineStoreTask() {
    return pipelineStoreTask;
  }
  public StageLibraryTask getStageLibraryTask() {
    return stageLibraryTask;
  }
  public WebServerTask getWebServerTask() {
    return webServerTask;
  }

}
