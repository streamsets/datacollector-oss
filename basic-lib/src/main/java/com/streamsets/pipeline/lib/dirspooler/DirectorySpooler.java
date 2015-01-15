/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.dirspooler;

import com.codahale.metrics.Meter;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DirectorySpooler {
  private static final Logger LOG = LoggerFactory.getLogger(DirectorySpooler.class);

  public enum FilePostProcessing {NONE, DELETE, ARCHIVE}

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Source.Context context;
    private String spoolDir;
    private int maxSpoolFiles;
    private String pattern;
    private FilePostProcessing postProcessing;
    private String archiveDir;
    private long archiveRetentionMillis;
    private String errorArchiveDir;

    private Builder() {
      postProcessing = FilePostProcessing.NONE;
    }

    public Builder setContext(Source.Context context) {
      this.context = Preconditions.checkNotNull(context, "context cannot be null");
      return this;
    }

    public Builder setDir(String dir) {
      this.spoolDir = Preconditions.checkNotNull(dir, "dir cannot be null");
      Preconditions.checkArgument(new File(dir).isAbsolute(), Utils.format("dir '{}' must be an absolute path", dir));
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

    public Builder setPostProcessing(FilePostProcessing postProcessing) {
      this.postProcessing = Preconditions.checkNotNull(postProcessing, "postProcessing mode cannot be null");
      return this;
    }

    public Builder setArchiveDir(String dir) {
      this.archiveDir = Preconditions.checkNotNull(dir, "dir cannot be null");
      Preconditions.checkArgument(new File(dir).isAbsolute(), Utils.format("dir '{}' must be an absolute path", dir));
      return this;
    }

    public Builder setArchiveRetention(long minutes) {
      return setArchiveRetention(minutes, TimeUnit.MINUTES);
    }

    //for testing only
    Builder setArchiveRetention(long time, TimeUnit unit) {
      Preconditions.checkArgument(time >= 0, "archive retention must be zero or greater");
      Preconditions.checkNotNull(unit, "archive retention unit cannot be null");
      archiveRetentionMillis = TimeUnit.MILLISECONDS.convert(time, unit);
      return this;
    }

    public Builder setErrorArchiveDir(String dir) {
      this.errorArchiveDir = Preconditions.checkNotNull(dir, "edir cannot be null");
      Preconditions.checkArgument(new File(dir).isAbsolute(), Utils.format("dir '{}' must be an absolute path", dir));
      return this;
    }

    public DirectorySpooler build() {
      Preconditions.checkArgument(context != null, "context not specified");
      Preconditions.checkArgument(spoolDir != null, "spool dir not specified");
      Preconditions.checkArgument(maxSpoolFiles > 0, "max spool files not specified");
      Preconditions.checkArgument(pattern != null, "file pattern not specified");
      if (postProcessing == FilePostProcessing.ARCHIVE) {
        Preconditions.checkArgument(archiveDir != null, "archive dir not specified");
      }
      return new DirectorySpooler(context, spoolDir, maxSpoolFiles, pattern, postProcessing, archiveDir,
                                  archiveRetentionMillis, errorArchiveDir);
    }
  }

  private final Source.Context context;
  private final String spoolDir;
  private final int maxSpoolFiles;
  private final String pattern;
  private final FilePostProcessing postProcessing;
  private final String archiveDir;
  private final long archiveRetentionMillis;
  private final String errorArchiveDir;

  public DirectorySpooler(Source.Context context, String spoolDir, int maxSpoolFiles, String pattern,
      FilePostProcessing postProcessing, String archiveDir, long archiveRetentionMillis,
      String errorArchiveDir) {
    this.context = context;
    this.spoolDir = spoolDir;
    this.maxSpoolFiles = maxSpoolFiles;
    this.pattern = pattern;
    this.postProcessing = postProcessing;
    this.archiveDir = archiveDir;
    this.archiveRetentionMillis = archiveRetentionMillis;
    this.errorArchiveDir = errorArchiveDir;
  }

  private volatile String currentFile;
  private Path spoolDirPath;
  private Path archiveDirPath;
  private Path errorArchiveDirPath;
  private PathMatcher fileMatcher;
  private WatchService watchService;
  private PriorityBlockingQueue<Path> filesQueue;
  private Path previousFile;
  private ScheduledExecutorService scheduledExecutor;

  private Meter spoolQueueMeter;

  private volatile boolean running;

  volatile FilePurger purger;

  private void checkBaseDir(Path path) {
    Preconditions.checkState(path.isAbsolute(), Utils.format("Path '{}' is not an absolute path", path));
    Preconditions.checkState(Files.exists(path), Utils.format("Path '{}' does not exist", path));
    Preconditions.checkState(Files.isDirectory(path), Utils.format("Path '{}' is not a directory", path));
  }

  public void init(String currentFile) {
    this.currentFile = currentFile;
    try {
      FileSystem fs = FileSystems.getDefault();
      spoolDirPath = fs.getPath(spoolDir).toAbsolutePath();
      checkBaseDir(spoolDirPath);
      if (postProcessing == FilePostProcessing.ARCHIVE) {
        archiveDirPath = fs.getPath(archiveDir).toAbsolutePath();
        checkBaseDir(archiveDirPath);
      }
      if (errorArchiveDir != null) {
        errorArchiveDirPath = fs.getPath(errorArchiveDir).toAbsolutePath();
        checkBaseDir(errorArchiveDirPath);
      }

      LOG.debug("Spool directory '{}', file pattern '{}', current file '{}'", spoolDirPath, pattern, currentFile);
      String extraInfo = "";
      if (postProcessing == FilePostProcessing.ARCHIVE) {
        extraInfo = Utils.format(", archive directory '{}', retention '{}' minutes", archiveDirPath ,
                                 archiveRetentionMillis / 60 / 1000);
      }
      LOG.debug("Post processing mode '{}'{}", postProcessing, extraInfo);

      fileMatcher = fs.getPathMatcher("glob:" + pattern);

      watchService = fs.newWatchService();
      spoolDirPath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

      filesQueue = new PriorityBlockingQueue<>();

      spoolQueueMeter = context.createMeter("spoolQueue");

      running = true;

      handleOlderFiles();
      queueExistingFiles();

      scheduledExecutor = Executors.newScheduledThreadPool(2, new ThreadFactoryBuilder()
          .setNameFormat("directory-spool-archive-retention").setDaemon(true).build());
      scheduledExecutor.execute(new Watcher());
      if (postProcessing == FilePostProcessing.ARCHIVE && archiveRetentionMillis > 0) {
        purger = new FilePurger();
        scheduledExecutor.scheduleAtFixedRate(purger, 1, 1, TimeUnit.MINUTES);
      }


    } catch (IOException ex) {
      destroy();
      throw new RuntimeException(ex);
    }
  }

  public void destroy() {
    running = false;
    try {
      if (scheduledExecutor != null) {
        scheduledExecutor.shutdownNow();
        scheduledExecutor = null;
      }
    } catch (RuntimeException ex) {
      LOG.warn("Error during scheduledExecutor.shutdownNow(), {}", ex.getMessage(), ex);
    }
    try {
      if (watchService != null) {
        watchService.close();
        watchService = null;
      }
    } catch (IOException|RuntimeException ex) {
      LOG.warn("Error during watchService.close(), {}", ex.getMessage(), ex);
    }
  }

  public boolean isRunning() {
    return watchService != null;
  }

  public Source.Context getContext() {
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

  public FilePostProcessing getPostProcessing() {
    return postProcessing;
  }

  public String getArchiveDir() {
    return archiveDir;
  }

  public long getArchiveRetentionMillis() {
    return archiveRetentionMillis;
  }

  public String getCurrentFile() {
    return currentFile;
  }

  void addFileToQueue(Path file, boolean checkCurrent) {
    Preconditions.checkNotNull(file, "file cannot be null");
    if (checkCurrent) {
      Preconditions.checkState(currentFile.compareTo(file.getFileName().toString()) < 0);
    }
    if (!filesQueue.contains(file)) {
      if (filesQueue.size() >= maxSpoolFiles) {
        throw new IllegalStateException(Utils.format("Exceeded max number '{}' of queued files", maxSpoolFiles));
      }
      filesQueue.add(file);
      spoolQueueMeter.mark(filesQueue.size());
    } else {
      LOG.warn("File '{}' already in queue, ignoring", file);
    }
  }

  public File poolForFile(long wait, TimeUnit timeUnit) throws InterruptedException {
    Preconditions.checkArgument(wait >= 0, "wait must be zero or greater");
    Preconditions.checkNotNull(timeUnit, "timeUnit cannot be null");
    Preconditions.checkState(running, "Spool directory watcher not running");
    synchronized (this) {
      if (previousFile != null) {
        switch (postProcessing) {
          case NONE:
            LOG.debug("Previous file '{}' remains in spool directory", previousFile);
            break;
          case DELETE:
            try {
              LOG.debug("Deleting previous file '{}'", previousFile);
              Files.delete(previousFile);
            } catch (IOException ex) {
              throw new RuntimeException(Utils.format("Could not delete file '{}', {}", previousFile, ex.getMessage(),
                                                      ex));
            }
            break;
          case ARCHIVE:
            try {
              if (Files.exists(previousFile)) {
                LOG.debug("Archiving previous file '{}'", previousFile);
                Files.move(previousFile, archiveDirPath.resolve(previousFile.getFileName()));
              }
            } catch (IOException ex) {
              throw new RuntimeException(Utils.format("Could not move file '{}' to archive dir {}, {}", previousFile,
                                                      archiveDirPath, ex.getMessage(), ex));
            }
            break;
        }
        previousFile = null;
      }
    }
    Path next = null;
    try {
      LOG.debug("Polling for file, waiting '{}' ms", TimeUnit.MILLISECONDS.convert(wait, timeUnit));
      next = filesQueue.poll(wait, timeUnit);
    } catch (InterruptedException ex) {
      next = null;
    } finally {
      LOG.debug("Polling for file returned '{}'", next);
      if (next != null) {
        currentFile = next.getFileName().toString();
        previousFile = next;
      }
    }
    return (next != null) ? next.toFile() : null;
  }

  public void handleFileInError(File file) throws IOException {
    if (errorArchiveDirPath != null) {
      LOG.error("Archiving file in error '{}' in error archive directory '{}'", file, errorArchiveDirPath);
      Files.move(file.toPath(), errorArchiveDirPath.resolve(file.toPath().getFileName()));
    } else {
      LOG.error("Leaving file in error '{}' in spool directory", file);
    }
  }

  void queueExistingFiles() throws IOException {
    DirectoryStream<Path> matchingFile = Files.newDirectoryStream(spoolDirPath, new DirectoryStream.Filter<Path>() {
      @Override
      public boolean accept(Path entry) throws IOException {
        return fileMatcher.matches(entry.getFileName()) &&
               (currentFile == null || entry.getFileName().toString().compareTo(currentFile) >= 0);
      }
    });
    for (Path file : matchingFile) {
      LOG.debug("Found file '{}', during initialization", file);
      addFileToQueue(file, false);
    }
  }

  void handleOlderFiles() throws IOException {
    if (postProcessing != FilePostProcessing.NONE) {
      DirectoryStream<Path> matchingFile = Files.newDirectoryStream(spoolDirPath, new DirectoryStream.Filter<Path>() {
        @Override
        public boolean accept(Path entry) throws IOException {
          return fileMatcher.matches(entry.getFileName()) &&
                 (currentFile != null && entry.getFileName().toString().compareTo(currentFile) < 0);
        }
      });
      for (Path file : matchingFile) {
        switch (postProcessing) {
          case DELETE:
            LOG.debug("Deleting old file '{}'", file);
            Files.delete(file);
            break;
          case ARCHIVE:
            LOG.debug("Archiving old file '{}'", file);
            Files.move(file, archiveDirPath.resolve(file.getFileName()));
            break;
        }
      }
    }
  }

  class Watcher implements Runnable {

    @SuppressWarnings("unchecked")
    public void run() {
      try {
        while (running) {
          WatchKey key = watchService.take();
          for (WatchEvent<?> event : key.pollEvents()) {
            WatchEvent.Kind<?> kind = event.kind();
            if (kind == StandardWatchEventKinds.OVERFLOW) {
              LOG.error("Watcher cannot keep up with filesystem watcher notifications");
              running = false;
            } else {
              Path file = ((WatchEvent<Path>) event).context();
              if (fileMatcher.matches(file)) {
                file = spoolDirPath.resolve(file.getFileName());
                LOG.debug("Watcher Found new file '{}', via filesystem watcher", file);
                addFileToQueue(file, true);
              }
            }
          }
          if (!key.reset()) {
            LOG.error("Watcher, spool directory '{}' is not valid anymore", spoolDirPath);
            running = false;
          }
        }
      } catch (InterruptedException ex) {
        LOG.debug("File watcher interrupted, shutting down");
      }
    }

  }

  class FilePurger implements Runnable {

    @SuppressWarnings("unchecked")
    public void run() {
      LOG.debug("Starting archived files purging");
      final long timeThreshold = System.currentTimeMillis() - archiveRetentionMillis;
      DirectoryStream.Filter filter = new DirectoryStream.Filter<Path>() {
        @Override
        public boolean accept(Path entry) throws IOException {
          return fileMatcher.matches(entry.getFileName()) &&
                 (timeThreshold - Files.getLastModifiedTime(entry).toMillis() > 0);
        }
      };
      int purged = 0;
      try (DirectoryStream<Path> filesToDelete = Files.newDirectoryStream(archiveDirPath, filter)) {
        for (Path file : filesToDelete) {
          if (running) {
            LOG.debug("Deleting archived file '{}', exceeded retention time", file);
            try {
              Files.delete(file);
              purged++;
            } catch (IOException ex) {
              LOG.warn("Error while deleting file '{}': {}", file, ex.getMessage(), ex);
            }
          } else {
            LOG.debug("Spooler has been destroyed, stopping archived files purging half way");
            break;
          }
        }
      } catch (IOException ex) {
        LOG.warn("Error while scanning directory '{}' for archived files purging: {}", archiveDirPath, ex.getMessage(),
                 ex);
      }
      LOG.debug("Finished archived files purging, deleted '{}' files", purged);
    }

  }

}
