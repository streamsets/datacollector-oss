/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.antennadoctor.storage;

import com.google.common.io.Resources;
import com.streamsets.datacollector.antennadoctor.AntennaDoctorConstants;
import com.streamsets.datacollector.antennadoctor.bean.AntennaDoctorRuleBean;
import com.streamsets.datacollector.antennadoctor.bean.AntennaDoctorStorageBean;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Storage module for Antenna Doctor.
 *
 * The storage currently only loads static storage from build-in jar file. However in the future we will add support
 * for loading even remote tools.
 */
public class AntennaDoctorStorage extends AbstractTask {
  private static final Logger LOG = LoggerFactory.getLogger(AntennaDoctorStorage.class);

  /**
   * Delegate when a new rules are available.
   *
   * This method is always called with all rules together (no incremental rule application here).
   */
  public interface NewRulesDelegate {
    void loadNewRules(List<AntennaDoctorRuleBean> rules);
  }

  private final NewRulesDelegate delegate;

  /**
   * Repository where we will store all our files.
   */
  private final Path repositoryDirectory;

  private final Configuration configuration;

  // Override file handling (if enabled in configuration)
  private ExecutorService overrideService;
  private OverrideFileRunnable overrideRunnable;
  private Future overrideFuture;

  public AntennaDoctorStorage(
    Configuration configuration,
    String dataDir,
    NewRulesDelegate delegate
  ) {
    super("Antenna Doctor Storage");
    this.configuration = configuration;
    this.delegate = delegate;
    this.repositoryDirectory = Paths.get(dataDir, AntennaDoctorConstants.DIR_REPOSITORY);
  }

  @Override
  protected void initTask() {
    // Make sure that we have our own directory to operate in
    try {
      LOG.info("Repository location: {}", repositoryDirectory);
      Files.createDirectories(repositoryDirectory);
    } catch (IOException e) {
      LOG.error("Cant initialize repository: {}", e.getMessage(), e);
      return;
    }

    delegate.loadNewRules(getBuiltInRules());

    // Schedule override runnable if allowed in configuration
    if(configuration.get(AntennaDoctorConstants.CONF_OVERRIDE_ENABLE, AntennaDoctorConstants.DEFAULT_OVERRIDE_ENABLE)) {
      LOG.info("Enabling polling of {} to override the rule database", AntennaDoctorConstants.FILE_OVERRIDE);
      this.overrideService = Executors.newSingleThreadExecutor();
      this.overrideRunnable = new OverrideFileRunnable();
      this.overrideFuture = overrideService.submit(this.overrideRunnable);

      // Also load all the changes from the override file
      if(Files.exists(repositoryDirectory.resolve(AntennaDoctorConstants.FILE_OVERRIDE))) {
        reloadOverrideFile();
      }
    }
  }

  @Override
  protected void stopTask() {
    if(overrideRunnable != null) {
      overrideRunnable.isStopped = true;
    }

    if(overrideFuture != null) {
      overrideFuture.cancel(true);
      try {
        overrideFuture.get();
      } catch (InterruptedException|ExecutionException e) {
        LOG.error("Error when stopping override file thread", e);
      }
    }

    if(overrideService != null) {
      overrideService.shutdownNow();
    }
  }

  private List<AntennaDoctorRuleBean> getBuiltInRules() {
    try {
      AntennaDoctorStorageBean storageBean = ObjectMapperFactory.get().readValue(
        Resources.getResource(AntennaDoctorStorage.class, AntennaDoctorConstants.RESOURCE_DATABASE),
        AntennaDoctorStorageBean.class
      );
      return storageBean.getRules();
    } catch (Throwable e) {
      LOG.error("Can't load default rule list from the distribution.", e);
      return Collections.emptyList();
    }
  }

  private void reloadOverrideFile() {
    LOG.info("Reloading override file {}", AntennaDoctorConstants.FILE_OVERRIDE);
    try(InputStream stream = Files.newInputStream(repositoryDirectory.resolve(AntennaDoctorConstants.FILE_OVERRIDE))) {
      AntennaDoctorStorageBean storageBean = ObjectMapperFactory.get().readValue(stream, AntennaDoctorStorageBean.class);
      delegate.loadNewRules(mergeRuleBeans(getBuiltInRules(), storageBean.getRules()));
    } catch (Throwable e) {
      LOG.error("Can't load override file: {}", e.getMessage(), e);
    }
  }

  /**
   * Merge two rule lists together, newer ids wins in case of duplicates.
   */
  private static List<AntennaDoctorRuleBean> mergeRuleBeans(List<AntennaDoctorRuleBean> older, List<AntennaDoctorRuleBean> newer) {
    Map<String, AntennaDoctorRuleBean> ruleMap = older.stream().collect(Collectors.toMap(AntennaDoctorRuleBean::getUuid, i -> i));
    newer.forEach(r -> ruleMap.put(r.getUuid(), r));
    return new ArrayList<>(ruleMap.values());
  }

  /**
   * Override runnable that scans the underlying repository for our OVERRIDE file and there is a new or updated rule set reloads the database.
   */
  private class OverrideFileRunnable implements Runnable {
    boolean isStopped = false;

    @Override
    public void run() {
      LOG.debug("Starting scanner thread watching for changes in {}", AntennaDoctorConstants.FILE_OVERRIDE);
      Thread.currentThread().setName("Antenna Doctor Override Scanner Thread");

      try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
        repositoryDirectory.register(watcher, StandardWatchEventKinds.ENTRY_MODIFY);
        while (!isStopped) {
          WatchKey key;
          try {
            key = watcher.poll(1, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            LOG.debug("Recovering from interruption", e);
            continue;
          }

          if(key == null) {
            continue;
          }
          try {
            for (WatchEvent<?> event : key.pollEvents()) {
              WatchEvent.Kind<?> kind = event.kind();

              WatchEvent<Path> ev = (WatchEvent<Path>) event;
              if (kind != StandardWatchEventKinds.ENTRY_MODIFY) {
                continue;
              }

              if (ev.context().toString().equals(AntennaDoctorConstants.FILE_OVERRIDE)) {
                reloadOverrideFile();
              }
            }
          } finally {
            key.reset();
          }

        }
      } catch (Throwable e) {
        LOG.error("Issue when stopping the override scan thread", e);
      } finally {
        LOG.info("Stopping scanner thread for changes to {} file", AntennaDoctorConstants.FILE_OVERRIDE);
      }
    }
  }
}
