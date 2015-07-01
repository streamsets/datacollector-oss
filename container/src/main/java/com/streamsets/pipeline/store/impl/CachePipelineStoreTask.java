/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.store.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.dataCollector.execution.PipelineStateStore;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.RuleDefinitions;
import com.streamsets.pipeline.store.PipelineInfo;
import com.streamsets.pipeline.store.PipelineRevInfo;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.task.AbstractTask;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.PipelineDirectoryUtil;

public class CachePipelineStoreTask extends AbstractTask implements PipelineStoreTask {

  private final PipelineStoreTask pipelineStore;
  private final Map<String, PipelineInfo> pipelineInfoMap;

  @Inject
  public CachePipelineStoreTask(PipelineStoreTask pipelineStore) {
    super("cachePipelineStore");
    this.pipelineStore = pipelineStore;
    pipelineInfoMap = new HashMap<>();
  }

  @Override
  public void init() {
    pipelineStore.init();
    try {
      for (PipelineInfo info: pipelineStore.getPipelines()) {
        pipelineInfoMap.put(info.getName(), info);
      }
    } catch (PipelineStoreException e) {
      throw new RuntimeException(Utils.format("Cannot fetch list of pipelines due to: '{}'", e), e);
    }
  }

  @Override
  public void stop() {
    pipelineStore.stop();
    pipelineInfoMap.clear();
  }

  @Override
  public void registerListener(PipelineStateStore pipelineStateStore) {
    pipelineStore.registerListener(pipelineStateStore);
  }

  @Override
  public synchronized PipelineConfiguration create(String name, String description, String user) throws PipelineStoreException {
    PipelineConfiguration pipelineConf =  pipelineStore.create(name, description, user);
    pipelineInfoMap.put(name, pipelineConf.getInfo());
    return pipelineConf;
  }

  @Override
  public synchronized void delete(String name) throws PipelineStoreException {
    pipelineStore.delete(name);
    pipelineInfoMap.remove(name);
  }

  @Override
  public synchronized List<PipelineInfo> getPipelines() throws PipelineStoreException {
    return Collections.unmodifiableList(new ArrayList(pipelineInfoMap.values()));
  }

  @Override
  public synchronized PipelineInfo getInfo(String name) throws PipelineStoreException {
    if (!pipelineInfoMap.containsKey(name)) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
    } else {
      return pipelineInfoMap.get(name);
    }
  }

  @Override
  public List<PipelineRevInfo> getHistory(String name) throws PipelineStoreException {
    return pipelineStore.getHistory(name);
  }

  @Override
  public synchronized PipelineConfiguration save(String name, String user, String tag, String tagDescription,
    PipelineConfiguration pipeline) throws PipelineStoreException {
    PipelineConfiguration pipelineConf =  pipelineStore.save(name, user, tag, tagDescription, pipeline);
    pipelineInfoMap.put(PipelineDirectoryUtil.getEscapedPipelineName(name), pipelineConf.getInfo());
    return pipelineConf;
  }

  @Override
  public PipelineConfiguration load(String name, String tagOrRev) throws PipelineStoreException {
    return pipelineStore.load(name, tagOrRev);
  }

  @Override
  public synchronized boolean hasPipeline(String name) {
    return pipelineInfoMap.containsKey(name);
  }

  @Override
  public RuleDefinitions retrieveRules(String name, String tagOrRev) throws PipelineStoreException {
    return pipelineStore.retrieveRules(name, tagOrRev);
  }

  @Override
  public RuleDefinitions storeRules(String pipelineName, String tag, RuleDefinitions ruleDefinitions)
    throws PipelineStoreException {
    return pipelineStore.storeRules(pipelineName, tag, ruleDefinitions);
  }

  @Override
  public boolean deleteRules(String name) throws PipelineStoreException {
    return pipelineStore.deleteRules(name);
  }

  @VisibleForTesting
  PipelineStoreTask getActualStore()  {
    return pipelineStore;
  }

}
