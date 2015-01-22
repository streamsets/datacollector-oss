/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.observerstore.impl;

import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.io.DataStore;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.observerstore.ObserverStore;
import com.streamsets.pipeline.util.PipelineDirectoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

public class FileObserverStore implements ObserverStore {

  private static final Logger LOG = LoggerFactory.getLogger(FileObserverStore.class);

  private static final String RULES_FILE = "rules.json";

  private final RuntimeInfo runtimeInfo;

  public FileObserverStore(RuntimeInfo runtimeInfo) {
    this.runtimeInfo = runtimeInfo;
  }

  @Override
  public RuleDefinition storeRules(String pipelineName, String rev, RuleDefinition ruleDefinition) {
    LOG.trace("Writing rule definitions to '{}'", getRulesFile(pipelineName, rev).getAbsolutePath());
    try {
      ObjectMapperFactory.get().writeValue(new DataStore(getRulesFile(pipelineName, rev)).getOutputStream(),
        ruleDefinition);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return ruleDefinition;
  }

  @Override
  public RuleDefinition retrieveRules(String pipelineName, String rev) {
    if(!PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev).exists() ||
      !getRulesFile(pipelineName, rev).exists()) {
      return new RuleDefinition(Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST,
        Collections.EMPTY_LIST);
    }
    LOG.trace("Reading rule definitions from '{}'", getRulesFile(pipelineName, rev).getAbsolutePath());
    try {
      RuleDefinition ruleDefinition = ObjectMapperFactory.get().readValue(
        new DataStore(getRulesFile(pipelineName, rev)).getInputStream(), RuleDefinition.class);
      return ruleDefinition;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private File getRulesFile(String pipelineName, String rev) {
    return new File(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev), RULES_FILE);
  }

}
