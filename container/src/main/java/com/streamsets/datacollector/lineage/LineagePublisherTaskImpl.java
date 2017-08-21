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
package com.streamsets.datacollector.lineage;

import com.google.common.annotations.VisibleForTesting;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class LineagePublisherTaskImpl extends AbstractTask implements LineagePublisherTask {
  private static final Logger LOG = LoggerFactory.getLogger(LineagePublisherTaskImpl.class);

  // Other Data Collector tasks and injected objects
  private final Configuration configuration;
  private final StageLibraryTask stageLibraryTask;

  @VisibleForTesting
  LineagePublisherRuntime publisherRuntime;
  private ArrayBlockingQueue<LineageEvent> eventQueue;

  private ExecutorService executorService;
  private EventQueueConsumer consumerRunnable;
  private Future consumerFuture;


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
    // Will be null if no publisher is configured
    if(eventQueue != null) {
      eventQueue.add(event);
    }
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

    // Initialize blocking queue that will buffer data before sending them to lineage publisher
    int size = configuration.get(LineagePublisherConstants.CONFIG_LINEAGE_QUEUE_SIZE, LineagePublisherConstants.DEFAULT_LINEAGE_QUEUE_SIZE);
    eventQueue = new ArrayBlockingQueue<>(size);

    // And run the separate thread
    executorService = Executors.newSingleThreadExecutor();
    consumerRunnable = new EventQueueConsumer();
  }

  @Override
  protected void runTask() {
    // Will be null if no publisher is configured
    if(executorService != null) {
      consumerFuture = executorService.submit(consumerRunnable);
    }
  }

  @Override
  protected void stopTask() {
    if(consumerFuture != null) {
      consumerRunnable.continueRunning = false;
      try {
        consumerFuture.get();
      } catch (InterruptedException|ExecutionException e) {
        LOG.error("Exception while stopping consumer thread", e);
      }
    }

    if(executorService != null) {
      executorService.shutdown();
    }

    if(eventQueue != null) {
      List<LineageEvent> drainedEvents = new ArrayList<>();
      eventQueue.drainTo(drainedEvents);
      publisherRuntime.publishEvents(drainedEvents);
    }

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
    String defConfig = LineagePublisherConstants.configDef(name);
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

    List<LineagePublisher.ConfigIssue> issues;

    try {
      issues = publisherRuntime.init(context);
    } catch (Throwable t) {
      LOG.error("Failed initializing lineage publisher: {}", t.toString(), t);
      throw new RuntimeException("Failed initializing lineage publisher", t);
    }

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

  private class EventQueueConsumer implements Runnable {

    boolean continueRunning = true;

    @Override
    public void run() {
      LOG.info("Starting lineage event consumer");
      Thread.currentThread().setName("Lineage Publisher Consumer");

      while(continueRunning) {
        List<LineageEvent> drainedEvents = new ArrayList<>();

        // Try to get first event in blocking fashion with limited wait
        LineageEvent event;
        try {
          event = eventQueue.poll(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          LOG.info("Consumer thread interrupted while waiting for events to show up");
          continue;
        }

        // If the event is not null, there is something to process
        if(event != null) {
          drainedEvents.add(event);
          eventQueue.drainTo(drainedEvents);

          LOG.debug("Consuming {} lineage events", drainedEvents.size());
          try {
            publisherRuntime.publishEvents(drainedEvents);
          } catch (Throwable e) {
            LOG.error("Failed to publish events", e);
          }
        }
      }

      LOG.info("Lineage event consumer finished");
    }
  }

}
