/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.main;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.domainServer.DomainServerCallbackTask;
import com.streamsets.pipeline.http.WebServerTask;
import com.streamsets.pipeline.prodmanager.ProductionPipelineManagerTask;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.task.CompositeTask;

import javax.inject.Inject;

public class PipelineTask extends CompositeTask {

  @Inject
  public PipelineTask(StageLibraryTask library, PipelineStoreTask store, ProductionPipelineManagerTask pipelineManager,
      WebServerTask webServer, DomainServerCallbackTask domainControllerCallbackTask) {
    super("pipelineNode", ImmutableList.of(library, store, pipelineManager, webServer, domainControllerCallbackTask),
      true);
  }

}
