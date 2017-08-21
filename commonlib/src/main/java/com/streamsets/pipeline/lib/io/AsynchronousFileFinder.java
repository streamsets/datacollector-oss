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

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * It finds files matching the specified glob path pattern. Except for '**' all glob wildcards are supported at
 * any depth.
 * <p/>
 * This is an asynchronous implementation.
 */
public class AsynchronousFileFinder extends FileFinder {
  private final static Logger LOG = LoggerFactory.getLogger(AsynchronousFileFinder.class);

  private final static int MAX_QUEUE_SIZE = 1000;
  private final static int MAX_DRAIN_SIZE = 20;

  private final Path globPath;
  private final FileFinder fileFinder;
  private final BlockingQueue<Path> found;
  private final ScheduledExecutorService executor;
  private final boolean ownExecutor;
  private boolean firstFind;

  public AsynchronousFileFinder(final Path globPath, int scanIntervalSec) {
    this(globPath, scanIntervalSec, null, FileFilterOption.FILTER_REGULAR_FILES_ONLY);
  }

  // if a null executor is given the finder will create its own instance and destroy it on close()
  public AsynchronousFileFinder(
      final Path globPath,
      int scanIntervalSec,
      ScheduledExecutorService executor,
      FileFilterOption filterOption) {
    Utils.checkArgument(scanIntervalSec > 0, Utils.formatL("scanInterval must be greater than zero", scanIntervalSec));
    this.globPath = globPath;
    fileFinder = new SynchronousFileFinder(globPath, filterOption);
    found = new LinkedBlockingDeque<>();
    this.executor = (executor != null) ? executor : new SafeScheduledExecutorService(1, "FileFinder");
    ownExecutor = (executor == null);
    firstFind = true;
    Runnable finder = new Runnable() {
      @Override
      public void run() {
        if (found.size() < MAX_QUEUE_SIZE)    {
          try {
            Set<Path> newFiles = fileFinder.find();
            LOG.debug("Found '{}' new files for '{}'", newFiles.size(), globPath);
            found.addAll(newFiles);
          } catch (IOException ex) {
            LOG.error("Error while finding files for '{}': {}", globPath, ex.toString(), ex);
          }
        } else {
          LOG.error("Found queue is full ('{}' files), skipping finding files for '{}'", MAX_QUEUE_SIZE, globPath);
        }
      }
    };
    //doing a first run synchronous to have everything for the initial find.
    finder.run();
    this.executor.scheduleAtFixedRate(finder, scanIntervalSec, scanIntervalSec, TimeUnit.SECONDS);
    LOG.trace("<init>(globPath={}, scanInterval={})", globPath, scanIntervalSec);
  }

  @Override
  public synchronized Set<Path> find() throws IOException {
    Set<Path> newFound = new HashSet<>();
    if (!found.isEmpty()) {
      if (firstFind) {
        //the first time we run find() we don't cap the files to return so we have everything there is at the moment
        firstFind = false;
        found.drainTo(newFound);
      } else {
        found.drainTo(newFound, MAX_DRAIN_SIZE);
      }
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Found '{}' new files for '{}'", newFound.size(), globPath);
    }
    return newFound;
  }

  @Override
  public boolean forget(Path path) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Forgetting '{}' for '{}'", path, path);
    }
    return fileFinder.forget(path);
  }

  @Override
  public void close() {
    LOG.debug("Closing");
    if (ownExecutor) {
      executor.shutdownNow();
    }
  }

  @VisibleForTesting
  ScheduledExecutorService getExecutor() {
    return executor;
  }

}
