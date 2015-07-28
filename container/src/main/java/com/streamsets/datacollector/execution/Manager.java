/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution;

import com.streamsets.datacollector.execution.manager.PipelineManagerException;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.task.Task;

import java.util.List;

// one per SDC
public interface Manager extends Task {
  //the Manager receives a PipelineStore and a PipelineState instance at <init> time
  //the Manager has a threadpool used exclusively for previews and runners
  //it should have configurations for maximum concurrent previews and maximum running pipelines and based on that
  //size the threadpool

  // creates a new previewer which is keep in a cache using its ID. if the pipeline is in a running state no
  // previewer is created.
  // the Previewer is cached for X amount of time after the preview finishes and discarded afterwards
  // (using a last-access cache). the previewer is given a PreviewerListener at <init> time which will be used
  // by the previewer to signal the PreviewOutput has been given back to the client and the Previewer could be
  // eagerly removed from the cache.
  public Previewer createPreviewer(String user, String name, String rev) throws PipelineStoreException;

  // returns the previewer from the cache with the specified ID
  public Previewer getPreviewer(String previewerId);

  // creates a runner for a given pipeline, the runner will have the current state of the pipeline.
  public Runner getRunner(String user, String name, String rev) throws PipelineStoreException, PipelineManagerException;

  public List<PipelineState> getPipelines() throws PipelineStoreException;

  // returns if the pipeline is in a 'running' state (starting, stopping, running)
  public boolean isPipelineActive(String name, String rev) throws PipelineStoreException;

}
