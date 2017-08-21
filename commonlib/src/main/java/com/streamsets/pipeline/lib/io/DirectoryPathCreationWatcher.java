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
package com.streamsets.pipeline.lib.io;

import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The file <code>DirectoryPathCreationWatcher</code> is used to watch for paths yet to be created.
 * <br/>
 * If {@link #scanIntervalSecs} greater than 0, this will spawn another thread to look for paths.
 *
 * Use {@link #find()} to get newly foundPaths.
 */
public class DirectoryPathCreationWatcher{
  private static final Logger LOGGER = LoggerFactory.getLogger(DirectoryPathCreationWatcher.class);

  private final Collection<Path> pathsWatched = Collections.synchronizedCollection(new TreeSet<Path>());
  private final ConcurrentLinkedQueue<Path> foundPathsQueue = new ConcurrentLinkedQueue<Path>();
  private final int scanIntervalSecs;
  private final Runnable finder;

  private ScheduledExecutorService executor = null;

  public DirectoryPathCreationWatcher(Collection<Path> paths, int scanIntervalSecs){
    this.scanIntervalSecs = scanIntervalSecs;
    pathsWatched.addAll(paths);
    finder = new Runnable() {
      @Override
      public void run() {
        synchronized (pathsWatched) {
          Iterator<Path> dirPathIterator = pathsWatched.iterator();
          while (dirPathIterator.hasNext()) {
            Path dirPath = dirPathIterator.next();
            if (Files.exists(dirPath) && Files.isDirectory(dirPath)) {
              foundPathsQueue.offer(dirPath);
              dirPathIterator.remove();
              LOGGER.debug("Found Path : {}", dirPath.toAbsolutePath().toString());
            }
          }
        }
      }
    };

    //Start finding the directory in the background.
    if (scanIntervalSecs > 0) {
      executor = new SafeScheduledExecutorService(1, "Directory Creation Watcher");
      executor.scheduleWithFixedDelay(finder, 0, scanIntervalSecs, TimeUnit.SECONDS);
    }
  }

  public Set<Path> find() {
    //During Synchronous run.
    if (scanIntervalSecs == 0) {
      finder.run();
    }

    Set<Path> foundPaths = new HashSet<Path>();
    Path detectedPath;
    while ( (detectedPath = foundPathsQueue.poll()) != null) {
      foundPaths.add(detectedPath);
    }

    if (pathsWatched.isEmpty()) {
      LOGGER.debug("All Directory Paths are found");
      close();
    }
    return foundPaths;
  }

  public void close() {
    if (executor != null) {
      LOGGER.debug("Directory Creation Watcher - Closing");
      executor.shutdownNow();
      executor = null;
    }
  }
}
