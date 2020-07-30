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
package com.streamsets.datacollector.store.impl;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.PipelineFragmentConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.execution.StateEventListener;
import com.streamsets.datacollector.restapi.bean.PipelineEnvelopeJson;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineRevInfo;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.PipelineException;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SlavePipelineStoreTask  implements PipelineStoreTask {

  private final PipelineStoreTask pipelineStore;

  public SlavePipelineStoreTask(PipelineStoreTask pipelineStore) {
    this.pipelineStore = pipelineStore;
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
  ) throws PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete(String name) throws PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<PipelineInfo> getPipelines() throws PipelineStoreException {
    return pipelineStore.getPipelines();
  }

  @Override
  public PipelineInfo getInfo(String name) throws PipelineException {
    return pipelineStore.getInfo(name);
  }

  @Override
  public List<PipelineRevInfo> getHistory(String name) throws PipelineException {
    return pipelineStore.getHistory(name);
  }

  @Override
  public PipelineConfiguration save(String name, String user, String tag, String tagDescription,
    PipelineConfiguration pipeline, boolean encryptCredentials) throws PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PipelineConfiguration load(String name, String tagOrRev) throws PipelineException {
    return pipelineStore.load(name, tagOrRev);
  }

  @Override
  public boolean hasPipeline(String name) throws PipelineException {
    return pipelineStore.hasPipeline(name);
  }

  @Override
  public RuleDefinitions retrieveRules(String name, String tagOrRev) throws PipelineException {
    return pipelineStore.retrieveRules(name, tagOrRev);
  }

  @Override
  public RuleDefinitions storeRules(
      String pipelineName,
      String tag,
      RuleDefinitions ruleDefinitions,
      boolean draft
  )
    throws PipelineException {
    return pipelineStore.storeRules(pipelineName, tag, ruleDefinitions, draft);
  }

  @Override
  public boolean deleteRules(String name) throws PipelineException {
    return pipelineStore.deleteRules(name);
  }

  @Override
  public String getName() {
    return "SlavePipelineStoreTask";
  }

  @Override
  public void init() {
    pipelineStore.init();
  }

  @Override
  public void run() {
    pipelineStore.run();
  }

  @Override
  public void waitWhileRunning() throws InterruptedException {
    pipelineStore.waitWhileRunning();
  }

  @Override
  public void stop() {
    pipelineStore.stop();
  }

  @Override
  public Status getStatus() {
    return pipelineStore.getStatus();
  }

  @Override
  public void saveUiInfo(String name, String rev, Map<String, Object> uiInfo) throws PipelineStoreException {
    //NOP
  }

  @Override
  public PipelineConfiguration saveMetadata(
      String user,
      String name,
      String rev,
      Map<String, Object> metadata
  ) throws PipelineStoreException {
    //NOP
    return null;
  }

  @Override
  public void registerStateListener(StateEventListener stateListener) {
  }

  @Override
  public boolean isRemotePipeline(String name, String rev) throws PipelineStoreException {
    return false;
  }

  @Override
  public PipelineFragmentConfiguration createPipelineFragment(
      String user,
      String pipelineId,
      String pipelineTitle,
      String description,
      boolean draft
  ) throws PipelineException {
    return pipelineStore.createPipelineFragment(user, pipelineId, pipelineTitle, description, draft);
  }

  @Override
  public List<PipelineInfo> getSamplePipelines() {
    return Collections.emptyList();
  }

  @Override
  public PipelineEnvelopeJson loadSamplePipeline(String samplePipelineId) {
    return null;
  }

}
