/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.store.impl;

import java.io.File;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.RuleDefinitions;
import com.streamsets.pipeline.store.PipelineInfo;
import com.streamsets.pipeline.store.PipelineRevInfo;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.task.AbstractTask;

public class SlavePipelineStoreTask  extends AbstractTask implements PipelineStoreTask {

  private final PipelineStoreTask pipelineStore;

  public SlavePipelineStoreTask(PipelineStoreTask pipelineStore) {
    super("SlavePipelineStore");
    this.pipelineStore = pipelineStore;
  }

  @Override
  public void init() {
    pipelineStore.init();
  }

  @Override
  public void stop() {
    pipelineStore.stop();
  }

  @Override
  public PipelineConfiguration create(String name, String description, String user) throws PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete(String name) throws PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<PipelineInfo> getPipelines() throws PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PipelineInfo getInfo(String name) throws PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<PipelineRevInfo> getHistory(String name) throws PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PipelineConfiguration save(String name, String user, String tag, String tagDescription,
    PipelineConfiguration pipeline) throws PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PipelineConfiguration load(String name, String tagOrRev) throws PipelineStoreException {
    return pipelineStore.load(name, tagOrRev);
  }

  @Override
  public boolean hasPipeline(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RuleDefinitions retrieveRules(String name, String tagOrRev) throws PipelineStoreException {
    return null;
  }

  @Override
  public RuleDefinitions storeRules(String pipelineName, String tag, RuleDefinitions ruleDefinitions)
    throws PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean deleteRules(String name) throws PipelineStoreException {
    throw new UnsupportedOperationException();
  }

}
