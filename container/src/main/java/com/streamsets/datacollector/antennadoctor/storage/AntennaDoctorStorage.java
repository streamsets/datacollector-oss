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
import com.streamsets.datacollector.antennadoctor.bean.AntennaDoctorRepositoryManifestBean;
import com.streamsets.datacollector.antennadoctor.bean.AntennaDoctorRepositoryUpdateBean;
import com.streamsets.datacollector.antennadoctor.bean.AntennaDoctorRuleBean;
import com.streamsets.datacollector.antennadoctor.bean.AntennaDoctorStorageBean;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.Configuration;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
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
  private final BuildInfo buildInfo;
  private final String productName;
  private final Configuration configuration;

  // Various threads that we might be running
  private ExecutorService executorService;
  private OverrideFileRunnable overrideRunnable;
  private UpdateRunnable updateRunnable;
  private Future future;

  public AntennaDoctorStorage(
    String productName,
    BuildInfo buildInfo,
    Configuration configuration,
    String dataDir,
    NewRulesDelegate delegate
  ) {
    super("Antenna Doctor Storage");
    this.productName = productName;
    this.buildInfo = buildInfo;
    this.configuration = configuration;
    this.delegate = delegate;
    this.repositoryDirectory = Paths.get(dataDir, AntennaDoctorConstants.DIR_REPOSITORY);
  }

  @Override
  protected void initTask() {
    LOG.info("Repository location: {}", repositoryDirectory);

    try {
      // Make sure that we have our own directory to operate in
      if(!Files.exists(repositoryDirectory)) {
        Files.createDirectories(repositoryDirectory);
      }

      Path store = repositoryDirectory.resolve(AntennaDoctorConstants.FILE_DATABASE);
      if(!Files.exists(store)) {
        try(OutputStream stream = Files.newOutputStream(store)) {
          Resources.copy(Resources.getResource(AntennaDoctorStorage.class, AntennaDoctorConstants.FILE_DATABASE), stream);
        }
      }
    } catch (IOException e) {
      LOG.error("Cant initialize repository: {}", e.getMessage(), e);
      return;
    }

    // Schedule override runnable if allowed in configuration
    if(configuration.get(AntennaDoctorConstants.CONF_OVERRIDE_ENABLE, AntennaDoctorConstants.DEFAULT_OVERRIDE_ENABLE)) {
      LOG.info("Enabling polling of {} to override the rule database", AntennaDoctorConstants.FILE_OVERRIDE);
      this.executorService = Executors.newSingleThreadExecutor();
      this.overrideRunnable = new OverrideFileRunnable();
      this.future = executorService.submit(this.overrideRunnable);
    }

    // Remote repo handling
    if(configuration.get(AntennaDoctorConstants.CONF_UPDATE_ENABLE, AntennaDoctorConstants.DEFAULT_UPDATE_ENABLE)) {
      if(overrideRunnable != null) {
        LOG.info("Using override, not starting update thread.");
      } else {
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.updateRunnable = new UpdateRunnable();
        this.future = ((ScheduledExecutorService)executorService).scheduleAtFixedRate(
          updateRunnable,
          configuration.get(AntennaDoctorConstants.CONF_UPDATE_DELAY, AntennaDoctorConstants.DEFAULT_UPDATE_DELAY),
          configuration.get(AntennaDoctorConstants.CONF_UPDATE_PERIOD, AntennaDoctorConstants.DEFAULT_UPDATE_PERIOD),
          TimeUnit.MINUTES
        );
      }
    }

    // And finally load rules
    delegate.loadNewRules(loadRules());
  }

  @Override
  protected void stopTask() {
    if(overrideRunnable != null) {
      overrideRunnable.isStopped = true;
    }

    if(future != null) {
      try {
        future.cancel(true);
        future.get();
      } catch (Throwable e) {
        LOG.debug("Error when stopping override file thread", e);
      }
    }

    if(executorService != null) {
      try {
        executorService.shutdownNow();
      } catch (Throwable e) {
        LOG.debug("Error when stopping override file service", e);
      }
    }
  }

  private List<AntennaDoctorRuleBean> loadRules() {
    Path database = repositoryDirectory.resolve(overrideRunnable == null ? AntennaDoctorConstants.FILE_DATABASE : AntennaDoctorConstants.FILE_OVERRIDE);
    LOG.trace("Loading rules from: {}", database);

    try(InputStream inputStream = Files.newInputStream(database)) {
      AntennaDoctorStorageBean storageBean = ObjectMapperFactory.get().readValue(
        inputStream,
        AntennaDoctorStorageBean.class
      );

      // Version protection
      if(storageBean.getSchemaVersion() != AntennaDoctorStorageBean.CURRENT_SCHEMA_VERSION) {
        LOG.error(
            "Ignoring the knowledge base file since it has incompatible schema version ({} versus expected {})",
            storageBean.getSchemaVersion(),
            AntennaDoctorStorageBean.CURRENT_SCHEMA_VERSION
        );
        return Collections.emptyList();
      }

      return storageBean.getRules();
    } catch (Throwable e) {
      LOG.error("Can't load knowledge base from {}: ", database.getFileName().toString(), e);
      return Collections.emptyList();
    }
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
                delegate.loadNewRules(loadRules());
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

  private class UpdateRunnable implements Runnable {
    @Override
    public void run() {
      String repoURL = configuration.get(AntennaDoctorConstants.CONF_UPDATE_URL, AntennaDoctorConstants.DEFAULT_UPDATE_URL) +
        "/" +
        productName +
        "/" +
        AntennaDoctorRepositoryManifestBean.CURRENT_SCHEMA_VERSION +
        "/"
      ;

      boolean changedApplied = false;
      try {
        LOG.info("Downloading updates from: {}", repoURL);

        // Download repo manifest first
        AntennaDoctorRepositoryManifestBean manifestBean;
        try(Response response = ClientBuilder.newClient()
          .target(repoURL + AntennaDoctorConstants.URL_MANIFEST)
          .queryParam("version", buildInfo.getVersion())
          .request()
          .get()) {

          manifestBean = ObjectMapperFactory.get().readValue(
            response.readEntity(InputStream.class),
            AntennaDoctorRepositoryManifestBean.class
          );

          if(manifestBean.getSchemaVersion() != AntennaDoctorRepositoryManifestBean.CURRENT_SCHEMA_VERSION) {
            LOG.error(
                "Ignoring remote knowledge base repository as it has incompatible schema version ({} versus expected {})",
                manifestBean.getSchemaVersion(),
                AntennaDoctorRepositoryManifestBean.CURRENT_SCHEMA_VERSION
            );
            return;
          }
        }
        LOG.debug("Base version in remote server is {}", manifestBean.getBaseVersion());

        AntennaDoctorStorageBean currentStore;
        try(InputStream stream = Files.newInputStream(repositoryDirectory.resolve(AntennaDoctorConstants.FILE_DATABASE))) {
          currentStore = ObjectMapperFactory.get().readValue(stream, AntennaDoctorStorageBean.class);
        }

        if(!currentStore.getBaseVersion().equals(manifestBean.getBaseVersion())) {
          LOG.info("Current base version ({}) is different then remote repo base version ({}), downloading remote version", currentStore.getBaseVersion(), manifestBean.getBaseVersion());
          try(Response response = ClientBuilder.newClient()
            .target(repoURL + manifestBean.getBaseVersion() + AntennaDoctorConstants.URL_VERSION_END)
            .request()
            .get()) {

            // And override current store
            try(InputStream gzipStream = new GzipCompressorInputStream(response.readEntity(InputStream.class))) {
              currentStore = ObjectMapperFactory.get().readValue(
                gzipStream,
                AntennaDoctorStorageBean.class
              );
            }

            currentStore.setBaseVersion(manifestBean.getBaseVersion());;
            currentStore.setUpdates(new LinkedList<>());
            changedApplied = true;
          }
        } else {
          LOG.debug("Current version {} matches server version", currentStore.getBaseVersion());
        }

        // Rule map for easier application of updates
        Map<String, AntennaDoctorRuleBean> ruleMap = currentStore.getRules().stream().collect(Collectors.toMap(AntennaDoctorRuleBean::getUuid, i -> i));

        // Now let's apply updates
        for(String update: manifestBean.getUpdates()) {
          if(currentStore.getUpdates().contains(update)) {
            LOG.debug("Update {} already applied", update);
            continue;
          }

          LOG.debug("Downloading update {} ", update);
          // Download the update bean
          AntennaDoctorRepositoryUpdateBean updateBean;
          try(Response response = ClientBuilder.newClient()
            .target(repoURL + update + AntennaDoctorConstants.URL_VERSION_END)
            .request()
            .get()) {

            // And override current store
            try (InputStream gzipStream = new GzipCompressorInputStream(response.readEntity(InputStream.class))) {
              updateBean = ObjectMapperFactory.get().readValue(
                gzipStream,
                AntennaDoctorRepositoryUpdateBean.class
              );
            }
          }

          updateBean.getUpdates().forEach(r -> ruleMap.put(r.getUuid(), r));
          updateBean.getDeletes().forEach(ruleMap::remove);
          currentStore.getUpdates().add(update);

          LOG.debug("Update {} successfully applied", update);
          changedApplied = true;
        }

        if(changedApplied) {
          LOG.info("Applied new changes");
          // Materialize rule list
          currentStore.setRules(new ArrayList<>(ruleMap.values()));

          // And finally write the updated repo file down
          try (OutputStream outputStream = Files.newOutputStream(repositoryDirectory.resolve(AntennaDoctorConstants.FILE_DATABASE))) {
            ObjectMapperFactory.get().writeValue(outputStream, currentStore);
          }

          delegate.loadNewRules(currentStore.getRules());
        } else {
          LOG.info("No new changes");
        }
      } catch (Throwable e) {
        LOG.error("Failed to retrieve updates: {}", e.getMessage(), e);
      }
    }
  }
}
