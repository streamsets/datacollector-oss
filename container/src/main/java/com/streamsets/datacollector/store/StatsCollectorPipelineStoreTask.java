/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.store;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.PipelineFragmentConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.execution.StateEventListener;
import com.streamsets.datacollector.restapi.bean.PipelineEnvelopeJson;
import com.streamsets.datacollector.usagestats.StatsCollector;
import com.streamsets.datacollector.util.PipelineException;

import java.util.List;
import java.util.Map;

public class StatsCollectorPipelineStoreTask implements PipelineStoreTask {
  private final PipelineStoreTask store;
  private final StatsCollector statsCollector;

  public StatsCollectorPipelineStoreTask(
      PipelineStoreTask store,
      StatsCollector statsCollector
  ) {
    this.store = store;
    this.statsCollector = statsCollector;
  }

  @Override
  public PipelineConfiguration create(
      String user,
      String pipelineId,
      String pipelineTitle,
      String description,
      boolean isRemote,
      boolean draft,
      Map<String, Object> metadata
  ) throws PipelineException {
    statsCollector.createPipeline(pipelineId);
    return store.create(user, pipelineId, pipelineTitle, description, isRemote, draft, metadata);
  }

  @Override
  public void delete(String name) throws PipelineException {
    store.delete(name);
  }

  @Override
  public List<PipelineInfo> getPipelines() throws PipelineStoreException {
    return store.getPipelines();
  }

  @Override
  public PipelineInfo getInfo(String name) throws PipelineException {
    return store.getInfo(name);
  }

  @Override
  public List<PipelineRevInfo> getHistory(String name) throws PipelineException {
    return store.getHistory(name);
  }

  @Override
  public PipelineConfiguration save(
      String user,
      String name,
      String tag,
      String tagDescription,
      PipelineConfiguration pipeline,
      boolean encryptCredentials
  ) throws PipelineException {
    return store.save(user, name, tag, tagDescription, pipeline, encryptCredentials);
  }

  @Override
  public PipelineConfiguration load(String name, String tagOrRev) throws PipelineException {
    return store.load(name, tagOrRev);
  }

  @Override
  public boolean hasPipeline(String name) throws PipelineException {
    return store.hasPipeline(name);
  }

  @Override
  public RuleDefinitions retrieveRules(String name, String tagOrRev) throws PipelineException {
    return store.retrieveRules(name, tagOrRev);
  }

  @Override
  public RuleDefinitions storeRules(
      String pipelineName,
      String tag,
      RuleDefinitions ruleDefinitions,
      boolean draft
  ) throws PipelineException {
    return store.storeRules(pipelineName, tag, ruleDefinitions, draft);
  }

  @Override
  public boolean deleteRules(String name) throws PipelineException {
    return store.deleteRules(name);
  }

  @Override
  public boolean isRemotePipeline(String name, String rev) throws PipelineStoreException {
    return store.isRemotePipeline(name, rev);
  }

  @Override
  public void saveUiInfo(String name, String rev, Map<String, Object> uiInfo) throws PipelineException {
    store.saveUiInfo(name, rev, uiInfo);
  }

  @Override
  public PipelineConfiguration saveMetadata(
      String user,
      String name,
      String rev,
      Map<String, Object> metadata
  ) throws PipelineException {
    return store.saveMetadata(user, name, rev, metadata);
  }

  @Override
  public void registerStateListener(StateEventListener stateListener) {
    store.registerStateListener(stateListener);
  }

  @Override
  public PipelineFragmentConfiguration createPipelineFragment(
      String user,
      String pipelineId,
      String pipelineTitle,
      String description,
      boolean draft
  ) throws PipelineException {
    return store.createPipelineFragment(user, pipelineId, pipelineTitle, description, draft);
  }

  @Override
  public String getName() {
    return store.getName();
  }

  @Override
  public void init() {
    store.init();
  }

  @Override
  public void run() {
    store.run();
  }

  @Override
  public void waitWhileRunning() throws InterruptedException {
    store.waitWhileRunning();
  }

  @Override
  public void stop() {
    store.stop();
  }

  @Override
  public Status getStatus() {
    return store.getStatus();
  }

  @Override
  public List<PipelineInfo> getSamplePipelines() throws PipelineStoreException {
    return store.getSamplePipelines();
  }

  @Override
  public PipelineEnvelopeJson loadSamplePipeline(String samplePipelineId) throws PipelineException {
    return store.loadSamplePipeline(samplePipelineId);
  }
}
