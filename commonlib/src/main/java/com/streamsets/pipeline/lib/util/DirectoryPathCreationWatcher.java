/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.pipeline.lib.util;

import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The file <code>DirectoryPathCreationWatcher</code> is used to
 * watch for the paths to be created and notify when it is created.
 * <br/>
 * If run in a seperate thread use the {@link #getCompletedPaths()} to
 * poll for completely detected paths at that time.
 */
public class DirectoryPathCreationWatcher implements Runnable{
  static final Logger DIR_PATH_CREATION_LOGGER = LoggerFactory.getLogger(
      DirectoryPathCreationWatcher.class.getCanonicalName()
  );

  private final WatchService watcher;
  private final Collection<Path> paths;

  private Set<Path> remainingPathsToWatchFor = new HashSet<Path>();
  private Map<WatchKey, Path> watchKeys = new HashMap<WatchKey, Path>();
  private Map<Path, Path> existingPathToWholePath = new HashMap<Path, Path>();

  private ConcurrentLinkedQueue<Path> completedPaths = new ConcurrentLinkedQueue<Path>();

  public DirectoryPathCreationWatcher(Collection<Path> paths) throws IOException{
    this.paths = paths;
    this.watcher = FileSystems.getDefault().newWatchService();
  }

  public ConcurrentLinkedQueue<Path> getCompletedPaths() {
    return this.completedPaths;
  }

  /**
   * Find part of the path which is already present.
   * @return Existing path.
   */
  private Path findExistingPartOfPath(Path currentPath) {
    Path cPath = currentPath;
    while (!Files.exists(cPath)) {
      cPath = cPath.getParent();
    }
    return cPath;
  }

  private void updateState(Path path) throws IOException {
    Path existingPath = findExistingPartOfPath(path);
    if (existingPath.equals(path)) {
      completedPaths.offer(path);
      remainingPathsToWatchFor.remove(path);
    } else {
      remainingPathsToWatchFor.add(path);
      WatchKey currentKey = existingPath.register(watcher, StandardWatchEventKinds.ENTRY_CREATE);
      watchKeys.put(currentKey, existingPath);
      existingPathToWholePath.put(existingPath, path);
    }
  }

  /**
   * Wait till the whole directory path is created.
   */
  @SuppressWarnings("unchecked")
  public void waitForDirectoryCreation() throws InterruptedException, IOException{
    try {
      //Initialize
      for (Path path : this.paths) {
        updateState(path);
      }

      while (!remainingPathsToWatchFor.isEmpty()) {
        WatchKey key = watcher.take();
        if (watchKeys.containsKey(key)) {
          Path currentPath = watchKeys.get(key);
          for (WatchEvent<?> event : key.pollEvents()) {
            WatchEvent<Path> ev = (WatchEvent<Path>) (event);
            if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE) {
              Path name = ev.context();
              Path child = currentPath.resolve(name);
              Path wholePath = existingPathToWholePath.get(currentPath);
              if (wholePath.equals(child) || wholePath.startsWith(child)) {
                DIR_PATH_CREATION_LOGGER.info(Utils.format("Watched Path Detected: {}", currentPath.toAbsolutePath()));
                updateState(wholePath);
                key.cancel();
              }
            }
          }
          key.reset();
        }
      }
    } finally {
      watcher.close();
    }
  }

  @Override
  public void run() {
    try {
      waitForDirectoryCreation();
    } catch (InterruptedException | IOException e) {
      DIR_PATH_CREATION_LOGGER.error(Utils.format("Error with directory creation watcher:{}", e.toString()));
    }
  }
}
