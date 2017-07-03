/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.lineage;

import com.streamsets.datacollector.config.LineagePublisherDefinition;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineagePublisher;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;

public class LineagePublisherTaskImpl extends AbstractTask implements LineagePublisherTask {
  private static final Logger LOG = LoggerFactory.getLogger(LineagePublisherTaskImpl.class);

  private final Configuration configuration;
  private final StageLibraryTask stageLibraryTask;

  private LineagePublisherRuntime publisherRuntime;

  @Inject
  public LineagePublisherTaskImpl(
    Configuration configuration,
    StageLibraryTask stageLibraryTask
  ) {
    super("Lineage Publisher Task");
    this.configuration = configuration;
    this.stageLibraryTask = stageLibraryTask;
  }

  @Override
  public void publishEvent(LineageEvent event) {
    // TBD
  }

  @Override
  protected void initTask() {
    String lineagePluginsConfig = configuration.get(LineagePublisherConstants.CONFIG_LINEAGE_PUBLISHERS, null);
    if(StringUtils.isEmpty(lineagePluginsConfig)) {
      LOG.info("No publishers configured");
      return;
    }

    String[] lineagePlugins = lineagePluginsConfig.split(",");
    // This implementation is intentionally limited to only one plugin at the moment
    if(lineagePlugins.length != 1) {
      throw new IllegalStateException("Only one lineage publisher is supported at the moment");
    }
    String publisherName = lineagePlugins[0];
    LineagePublisherDefinition def = getDefinition(publisherName);
    LOG.info("Using lineage publisher named {} (backed by {}::{})", publisherName, def.getLibraryDefinition().getName(), def.getName());

    // Instantiate and initialize the publisher
    createAndInitializeRuntime(def, publisherName);
  }

  @Override
  protected void runTask() {
    LOG.info("Lineage Publisher Task is starting");
  }

  @Override
  protected void stopTask() {
    if(publisherRuntime != null) {
      publisherRuntime.destroy();
    }
  }

  /**
   * Parse given configuration declaration of lineage plugin and return appropriate definition.
   *
   * This method will throw exceptions on all error paths.
   */
  private LineagePublisherDefinition getDefinition(String name) {
    String defConfig = LineagePublisherConstants.CONFIG_LINEAGE_PUBLISHER_PREFIX + name + LineagePublisherConstants.CONFIG_LINEAGE_PUBSLIHER_DEF;
    String publisherDefinition = configuration.get(defConfig, null);
    if(StringUtils.isEmpty(publisherDefinition)) {
      throw new IllegalArgumentException(Utils.format("Missing definition '{}'", defConfig));
    }

    String []lineagePluginDefs = publisherDefinition.split("::");
    if(lineagePluginDefs.length != 2) {
      throw new IllegalStateException(Utils.format(
        "Invalid definition '{}', expected $libraryName::$publisherName",
        publisherDefinition
      ));
    }

    LineagePublisherDefinition def = stageLibraryTask.getLineagePublisherDefinition(
      lineagePluginDefs[0], // Library
      lineagePluginDefs[1]  // Plugin name
    );
    if(def == null) {
      throw new IllegalStateException(Utils.format("Can't find publisher '{}'", publisherDefinition));
    }

    return def;
  }

  private void createAndInitializeRuntime(LineagePublisherDefinition def, String name) {
    LineagePublisher publisher = instantiateLineagePublisher(def);
    this.publisherRuntime = new LineagePublisherRuntime(def, publisher);

    LineagePublisherContext context = new LineagePublisherContext(name, configuration);

    List<LineagePublisher.ConfigIssue> issues = publisherRuntime.init(context);
    // If the issues aren't empty, terminate the execution
    if(!issues.isEmpty()) {
      for(LineagePublisher.ConfigIssue issue : issues) {
        LOG.error("Lineage init issue: {}", issue);
      }
      throw new RuntimeException(Utils.format("Can't initialize lineage publisher ({} issues)", issues.size()));
    }
  }

  private LineagePublisher instantiateLineagePublisher(LineagePublisherDefinition def) {
    ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(def.getClassLoader());
      return def.getKlass().newInstance();
    } catch (IllegalAccessException|InstantiationException e) {
      LOG.error("Can't instantiate publisher", e);
      throw new RuntimeException("Can't instantiate publisher", e);
    } finally {
      Thread.currentThread().setContextClassLoader(previousClassLoader);
    }
  }

}
