/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * It finds files matching the specified glob path pattern. Except for '**' all glob wildcards are supported at
 * any depth.
 * <p/>
 * This is an asynchronous implementation.
 */
public class AsynchronousFileFinder implements FileFinder {
  private final static Logger LOG = LoggerFactory.getLogger(AsynchronousFileFinder.class);
  private final static int MAX_QUEUE_SIZE = 1000;
  private final static int MAX_DRAIN_SIZE = 20;

  private final Path path;
  private final FileFinder fileFinder;
  private final BlockingQueue<Path> found;
  private final SafeScheduledExecutorService executor;
  private final boolean ownExecutor;
  private boolean firstFind;

  public AsynchronousFileFinder(final Path globPath, int scanIntervalSec) {
    this(globPath, scanIntervalSec, null);
  }

  public AsynchronousFileFinder(final Path globPath, int scanIntervalSec, SafeScheduledExecutorService executor) {
    this.path = globPath;
    fileFinder = new SynchronousFileFinder(globPath);
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
            LOG.error("Error while finding files for '{}': {}", globPath, ex.getMessage(), ex);
          }
        } else {
          LOG.error("Found queue is full ('{}' files), skipping finding files for '{}'", MAX_QUEUE_SIZE, globPath);
        }
      }
    };
    //doing a first run synchronous to have everything for the initial find.
    finder.run();
    this.executor.scheduleAtFixedRate(finder, scanIntervalSec, scanIntervalSec, TimeUnit.SECONDS);
  }

  @Override
  public synchronized Set<Path> find() throws IOException {
    Set<Path> newFound = new HashSet<>();
    if (!found.isEmpty()) {
      if (firstFind) {
        //the first time we run find() we don't cap the files to return so we have everything there is at the moment
        firstFind = true;
        found.drainTo(newFound);
      } else {
        found.drainTo(newFound, MAX_DRAIN_SIZE);
      }
    }
    LOG.debug("Returning '{}' new files for '{}'", newFound.size(), path);
    return newFound;
  }

  @Override
  public boolean forget(Path path) {
    LOG.debug("Forgetting '{}' for '{}'", path, path);
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
  SafeScheduledExecutorService getExecutor() {
    return executor;
  }

}
