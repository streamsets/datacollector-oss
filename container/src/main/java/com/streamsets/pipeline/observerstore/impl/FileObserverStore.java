/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.observerstore.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.streamsets.pipeline.config.AlertDefinition;
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
import java.util.List;

public class FileObserverStore implements ObserverStore {

  private static final Logger LOG = LoggerFactory.getLogger(FileObserverStore.class);

  private static final String ALERTS_FILE = "alerts.json";

  private final RuntimeInfo runtimeInfo;

  public FileObserverStore(RuntimeInfo runtimeInfo) {
    this.runtimeInfo = runtimeInfo;
  }

  @Override
  public List<AlertDefinition> storeAlerts(String pipelineName, String rev, List<AlertDefinition> alerts) {
    LOG.trace("Writing alert definition to '{}'", getAlertFile(pipelineName, rev).getAbsolutePath());
    try {
      ObjectMapperFactory.get().writeValue(new DataStore(getAlertFile(pipelineName, rev)).getOutputStream(),
        alerts);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return alerts;
  }

  @Override
  public List<AlertDefinition> retrieveAlerts(String pipelineName, String rev) {
    if(!PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev).exists() ||
      !getAlertFile(pipelineName, rev).exists()) {
      return Collections.emptyList();
    }
    LOG.trace("Reading alert definition from '{}'", getAlertFile(pipelineName, rev).getAbsolutePath());
    try {
      List<AlertDefinition> alerts = ObjectMapperFactory.get().readValue(
        new DataStore(getAlertFile(pipelineName, rev)).getInputStream(), new TypeReference<List<AlertDefinition>>() {});
      return alerts;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private File getAlertFile(String pipelineName, String rev) {
    return new File(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev), ALERTS_FILE);
  }
}
