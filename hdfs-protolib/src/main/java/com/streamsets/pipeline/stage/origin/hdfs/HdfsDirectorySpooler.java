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
package com.streamsets.pipeline.stage.origin.hdfs;

import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

public class HdfsDirectorySpooler {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsDirectorySpooler.class);
  public static final String FILE_SEPARATOR = System.getProperty("file.separator");

  private final PushSource.Context context;
  private final String spoolDir;
  private final int maxSpoolFiles;
  private final String pattern;
  private final String firstFile;
  private final Comparator<FileStatus> pathComparator;
  private final FileSystem fileSystem;

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private PushSource.Context context;
    private String spoolDir;
    private int maxSpoolFiles;
    private String pattern;
    private FileSystem fileSystem;
    private String firstFile;

    private Builder() {
    }

    public Builder setContext(PushSource.Context context) {
      this.context = Preconditions.checkNotNull(context, "context cannot be null");
      return this;
    }

    public Builder setDir(String dir) {
      final String spoolDirInput = Preconditions.checkNotNull(dir, "dir cannot be null");
      final Path spoolDirPath = new Path(spoolDirInput);

      Preconditions.checkArgument(spoolDirPath.isAbsolute(), Utils.formatL("dir '{}' must be an absolute path", dir));
      // normalize path to ensure no trailing slash
      spoolDir = spoolDirPath.toString();
      return this;
    }

    public Builder setMaxSpoolFiles(int maxSpoolFiles) {
      Preconditions.checkArgument(maxSpoolFiles > 0, "maxSpoolFiles must be greater than zero");
      this.maxSpoolFiles = maxSpoolFiles;
      return this;
    }

    public Builder setFilePattern(String pattern) {
      this.pattern = Preconditions.checkNotNull(pattern, "pattern cannot be null");
      return this;
    }

    public Builder setFileSystem(FileSystem fileSystem) {
      this.fileSystem = fileSystem;
      return this;
    }

    public Builder setFirstFile(String firstFile) {
      this.firstFile = firstFile;
      return this;
    }

    public HdfsDirectorySpooler build() {
      Preconditions.checkArgument(context != null, "context not specified");
      Preconditions.checkArgument(spoolDir != null, "spool dir not specified");
      Preconditions.checkArgument(maxSpoolFiles > 0, "max spool files not specified");
      return new HdfsDirectorySpooler(
          context,
          spoolDir,
          pattern,
          firstFile,
          maxSpoolFiles,
          fileSystem
      );
    }
  }

  public HdfsDirectorySpooler(
      PushSource.Context context,
      String spoolDir,
      String pattern,
      String firstFile,
      int maxSpoolFiles,
      FileSystem fileSystem
  ) {
    this.context = context;
    this.spoolDir = spoolDir;
    this.maxSpoolFiles = maxSpoolFiles;
    this.pattern = pattern;
    this.firstFile = firstFile;
    this.fileSystem = fileSystem;

    pathComparator = new Comparator<FileStatus>() {
      @Override
      public int compare(FileStatus file1, FileStatus file2) {
          return file1.getPath().compareTo(file2.getPath());
      }
    };
  }

  private volatile FileStatus currentFile;

  private Path spoolDirPath;
  private PriorityBlockingQueue<FileStatus> filesQueue;
  private Path previousFile;
  private ScheduledExecutorService scheduledExecutor;

  private Meter spoolQueueMeter;

  private volatile boolean running;

  volatile FileFinder finder;

  public void init() {
    try {
      spoolDirPath = new Path(spoolDir);

      if (!Strings.isNullOrEmpty(firstFile)) {
        currentFile = fileSystem.getFileStatus(new Path(firstFile));
      }

      LOG.debug("Spool directory '{}', file pattern '{}', current file '{}'", spoolDirPath, pattern, currentFile);

      filesQueue = new PriorityBlockingQueue<>(11, pathComparator);

      spoolQueueMeter = context.createMeter("spoolQueue");

      startSpooling();
    } catch (IOException ex) {
      destroy();
      throw new RuntimeException(ex);
    }
  }

  private void startSpooling() throws IOException {
    running = true;

    scheduledExecutor = new SafeScheduledExecutorService(1, "directory-spooler");

    findAndQueueFiles();

    finder = new FileFinder();
    scheduledExecutor.scheduleAtFixedRate(finder, 5, 5, TimeUnit.SECONDS);
  }

  public void destroy() {
    running = false;
    try {
      if (scheduledExecutor != null) {
        scheduledExecutor.shutdownNow();
        scheduledExecutor = null;
      }
    } catch (RuntimeException ex) {
      LOG.warn("Error during scheduledExecutor.shutdownNow(), {}", ex.toString(), ex);
    }
  }

  public boolean isRunning() {
    return running;
  }

  public PushSource.Context getContext() {
    return context;
  }

  public String getSpoolDir() {
    return spoolDir;
  }

  public int getMaxSpoolFiles() {
    return maxSpoolFiles;
  }

  public String getFilePattern() {
    return pattern;
  }

  private void addFileToQueue(FileStatus file) {
    Preconditions.checkNotNull(file, "file cannot be null");
    boolean valid = currentFile != null && !StringUtils.isEmpty(currentFile.toString());

    if (!valid || (!filesQueue.contains(file) && pathComparator.compare(currentFile, file) <= 0)) {
      if (filesQueue.size() >= maxSpoolFiles) {
        throw new IllegalStateException(Utils.format("Exceeded max number '{}' of queued files", maxSpoolFiles));
      }

      filesQueue.add(file);
      spoolQueueMeter.mark(filesQueue.size());
      LOG.trace("Found file '{}'", file);
    } else {
      LOG.warn("Ignoring file '{}'", file);
    }
  }

  public synchronized FileStatus poolForFile(long wait, TimeUnit timeUnit) throws InterruptedException {
    Preconditions.checkArgument(wait >= 0, "wait must be zero or greater");
    Preconditions.checkNotNull(timeUnit, "timeUnit cannot be null");

    Preconditions.checkState(running, "Spool directory watcher not running");

    FileStatus next = null;
    try {
      LOG.debug("Polling for file, waiting '{}' ms", TimeUnit.MILLISECONDS.convert(wait, timeUnit));
      next = filesQueue.poll(wait, timeUnit);
    } catch (InterruptedException ex) {
      next = null;
    } finally {
      LOG.debug("Polling for file returned '{}'", next);
      if (next != null) {
        currentFile = next;
        previousFile = next.getPath();
      }
    }
    return (next != null) ? next : null;
  }

  @VisibleForTesting
  void findAndQueueFiles() throws IOException {
    List<FileStatus> foundFiles = new ArrayList<>(maxSpoolFiles);
    FileStatus[] matchingFile = fileSystem.globStatus(new Path(spoolDirPath + FILE_SEPARATOR + pattern));

    if (matchingFile == null) {
      LOG.trace("No files found");
      return;
    }

    for (FileStatus file : matchingFile) {
      if (!running) {
        return;
      }

      if (file.isFile()) {
        foundFiles.add(file);
      }

      if (foundFiles.size() > maxSpoolFiles) {
        throw new IllegalStateException(Utils.format("Exceeded max number '{}' of spool files in directory",
            maxSpoolFiles
        ));
      }
    }

    // sort the foundFiles
    foundFiles.sort(pathComparator);

    for (FileStatus file : foundFiles) {
      addFileToQueue(file);
      if (filesQueue.size() > maxSpoolFiles) {
        throw new IllegalStateException(Utils.format("Exceeded max number '{}' of spool files in directory",
            maxSpoolFiles
        ));
      }
    }
    spoolQueueMeter.mark(filesQueue.size());
    LOG.debug("Found '{}' files", filesQueue.size());
  }

  class FileFinder implements Runnable {

    public FileFinder(){
    }

    @Override
    public synchronized void run() {
      // by using current we give a chance to have unprocessed files out of order
      LOG.debug("Starting file finder from '{}'", currentFile);
      try {
        findAndQueueFiles();
      } catch (Exception ex) {
        LOG.warn("Error while scanning directory '{}' for files newer than '{}': {}", currentFile,
            ex.toString(), ex);
      }
    }
  }
}
