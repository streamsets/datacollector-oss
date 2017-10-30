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
package com.streamsets.pipeline.lib.dirspooler;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.lib.io.DirectoryPathCreationWatcher;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.streamsets.pipeline.lib.dirspooler.PathMatcherMode.GLOB;
import static com.streamsets.pipeline.lib.dirspooler.PathMatcherMode.REGEX;

public class DirectorySpooler {
  private static final Logger LOG = LoggerFactory.getLogger(DirectorySpooler.class);
  private static final String PENDING_FILES = "pending.files";

  private final PushSource.Context context;
  private final String spoolDir;
  private final int maxSpoolFiles;
  private final String pattern;
  private final PathMatcherMode pathMatcherMode;
  private final FilePostProcessing postProcessing;
  private final String archiveDir;
  private final long archiveRetentionMillis;
  private final String errorArchiveDir;
  private final boolean useLastModified;
  private final Comparator<Path> pathComparator;
  private final boolean processSubdirectories;

  public enum FilePostProcessing {NONE, DELETE, ARCHIVE}

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private PushSource.Context context;
    private String spoolDir;
    private int maxSpoolFiles;
    private String pattern;
    private PathMatcherMode pathMatcherMode = PathMatcherMode.GLOB;
    private FilePostProcessing postProcessing;
    private String archiveDir;
    private long archiveRetentionMillis;
    private String errorArchiveDir;
    private boolean waitForPathAppearance;
    private boolean useLastModifiedTimestamp;
    private boolean processSubdirectories;

    private Builder() {
      postProcessing = FilePostProcessing.NONE;
    }

    public Builder setContext(PushSource.Context context) {
      this.context = Preconditions.checkNotNull(context, "context cannot be null");
      return this;
    }

    public Builder setDir(String dir) {
      final String spoolDirInput = Preconditions.checkNotNull(dir, "dir cannot be null");
      final Path spoolDirPath = FileSystems.getDefault().getPath(spoolDirInput);
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

    public Builder setPathMatcherMode(PathMatcherMode mode) {
      this.pathMatcherMode = Preconditions.checkNotNull(mode, "path matcher mode cannot be null");
      return this;
    }

    public Builder setPostProcessing(FilePostProcessing postProcessing) {
      this.postProcessing = Preconditions.checkNotNull(postProcessing, "postProcessing mode cannot be null");
      return this;
    }

    public Builder setArchiveDir(String dir) {
      this.archiveDir = Preconditions.checkNotNull(dir, "dir cannot be null");
      Preconditions.checkArgument(new File(dir).isAbsolute(), Utils.formatL("dir '{}' must be an absolute path", dir));
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
      Preconditions.checkArgument(new File(dir).isAbsolute(), Utils.formatL("dir '{}' must be an absolute path", dir));
      return this;
    }

    public Builder waitForPathAppearance(boolean waitForPathAppearance) {
      this.waitForPathAppearance = waitForPathAppearance;
      return this;
    }

    public Builder processSubdirectories(boolean processSubdirectories) {
      this.processSubdirectories = processSubdirectories;
      return this;
    }

    public Builder setUseLastModifiedTimestamp(boolean useLastModifiedTimestamp) {
      this.useLastModifiedTimestamp = useLastModifiedTimestamp;
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
      return new DirectorySpooler(
          context,
          spoolDir,
          maxSpoolFiles,
          pattern,
          pathMatcherMode,
          postProcessing,
          archiveDir,
          archiveRetentionMillis,
          errorArchiveDir,
          waitForPathAppearance,
          useLastModifiedTimestamp,
          processSubdirectories
      );
    }
  }

  public DirectorySpooler(
      PushSource.Context context,
      String spoolDir,
      int maxSpoolFiles,
      String pattern,
      PathMatcherMode pathMatcherMode,
      FilePostProcessing postProcessing,
      String archiveDir,
      long archiveRetentionMillis,
      String errorArchiveDir,
      boolean processSubdirectories
  ) {
    this(
        context,
        spoolDir,
        maxSpoolFiles,
        pattern,
        pathMatcherMode,
        postProcessing,
        archiveDir,
        archiveRetentionMillis,
        errorArchiveDir,
        true,
        false,
        processSubdirectories
    );
  }

  public DirectorySpooler(
      PushSource.Context context,
      String spoolDir,
      int maxSpoolFiles,
      String pattern,
      PathMatcherMode pathMatcherMode,
      FilePostProcessing postProcessing,
      String archiveDir,
      long archiveRetentionMillis,
      String errorArchiveDir,
      boolean waitForPathAppearance,
      final boolean useLastModified,
      boolean processSubdirectories
  ) {
    this.context = context;
    this.spoolDir = spoolDir;
    this.maxSpoolFiles = maxSpoolFiles;
    this.pattern = pattern;
    this.pathMatcherMode = pathMatcherMode;
    this.postProcessing = postProcessing;
    this.archiveDir = archiveDir;
    this.archiveRetentionMillis = archiveRetentionMillis;
    this.errorArchiveDir = errorArchiveDir;
    this.waitForPathAppearance = waitForPathAppearance;
    this.useLastModified = useLastModified;
    this.processSubdirectories = processSubdirectories;

    pathComparator = new Comparator<Path>() {
      @Override
      public int compare(Path file1, Path file2) {
        try {
          if (useLastModified) {
            // if comparing with folder last modified timestamp, always return true
            if (file2.toString().isEmpty()) {
              return 1;
            }
            int compares = Files.getLastModifiedTime(file1).compareTo(Files.getLastModifiedTime(file2));
            if (compares != 0) {
              return compares;
            }
          }
          return file1.getFileName().compareTo(file2.getFileName());
        } catch (NoSuchFileException ex) {
          // Logged later, so don't log here.
          throw new RuntimeException(ex);
        } catch (IOException ex) {
          LOG.warn("Could not sort files due to IO Exception", ex);
          throw new RuntimeException(ex);
        }
      }
    };
  }

  private volatile Path currentFile;

  private Path spoolDirPath;
  private Path archiveDirPath;
  private Path errorArchiveDirPath;
  private PathMatcher fileMatcher;
  private PriorityBlockingQueue<Path> filesQueue;
  private Path previousFile;
  private ScheduledExecutorService scheduledExecutor;
  private boolean waitForPathAppearance;

  private Meter spoolQueueMeter;
  private Counter pendingFilesCounter;

  private volatile boolean running;

  volatile FilePurger purger;
  volatile FileFinder finder;

  private void checkBaseDir(Path path) {
    Preconditions.checkState(path.isAbsolute(), Utils.formatL("Path '{}' is not an absolute path", path));
    Preconditions.checkState(Files.exists(path), Utils.formatL("Path '{}' does not exist", path));
    Preconditions.checkState(Files.isDirectory(path), Utils.formatL("Path '{}' is not a directory", path));
  }

  public static PathMatcher createPathMatcher(String pattern, PathMatcherMode mode) {
    FileSystem fs = FileSystems.getDefault();
    PathMatcher matcher;
    if (mode == GLOB) {
      matcher = fs.getPathMatcher("glob:" + pattern);
    } else if (mode == REGEX) {
      matcher = fs.getPathMatcher("regex:" + pattern);
    } else {
      throw new IllegalArgumentException("Unrecognized Path Matcher Mode: " + mode.getLabel());
    }
    return matcher;
  }

  public void init(String sourceFile) {
    try {
      FileSystem fs = FileSystems.getDefault();
      spoolDirPath = fs.getPath(spoolDir).toAbsolutePath();

      if(StringUtils.isEmpty(sourceFile)) {
        sourceFile = "";
        this.currentFile = Paths.get(sourceFile);
      } else {
        // sourceFile can contain: a filename, a partial path (relative to spoolDirPath),
        // or a full path.
        this.currentFile = Paths.get(sourceFile);
        if (this.currentFile.getParent() == null
            || !(this.currentFile.getParent().toString().contains(spoolDirPath.toString()))) {
          // if filename only or not full path - add the full path to the filename
          this.currentFile = Paths.get(spoolDirPath.toString(), sourceFile);
        }
      }

      if (!waitForPathAppearance) {
        checkBaseDir(spoolDirPath);
      }
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
        extraInfo = Utils.format(", archive directory '{}', retention '{}' minutes", archiveDirPath,
            archiveRetentionMillis / 60 / 1000
        );
      }
      LOG.debug("Post processing mode '{}'{}", postProcessing, extraInfo);

      fileMatcher = createPathMatcher(pattern, pathMatcherMode);

      if (useLastModified) {
        // 11 is the DEFAULT_INITIAL_CAPACITY -- seems pretty random, but lets use the same one.
        filesQueue = new PriorityBlockingQueue<>(11, pathComparator);
      } else {
        filesQueue = new PriorityBlockingQueue<>();
      }

      spoolQueueMeter = context.createMeter("spoolQueue");

      pendingFilesCounter = context.createCounter(PENDING_FILES);

      if (!waitForPathAppearance) {
        startSpooling(currentFile);
      }

    } catch (IOException ex) {
      destroy();
      throw new RuntimeException(ex);
    }
  }

  private void startSpooling(Path currentFile) throws IOException {
    running = true;

    if(!context.isPreview()) {
      handleOlderFiles(currentFile);
    }

    scheduledExecutor = new SafeScheduledExecutorService(1, "directory-spooler");

    findAndQueueFiles(currentFile, true, false);

    finder = new FileFinder();
    scheduledExecutor.scheduleAtFixedRate(finder, 5, 5, TimeUnit.SECONDS);

    if (postProcessing == FilePostProcessing.ARCHIVE && archiveRetentionMillis > 0) {
      // create and schedule file purger only if the retention time is > 0
      purger = new FilePurger();
      scheduledExecutor.scheduleAtFixedRate(purger, 1, 1, TimeUnit.MINUTES);
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

  public FilePostProcessing getPostProcessing() {
    return postProcessing;
  }

  public String getArchiveDir() {
    return archiveDir;
  }

  private void addFileToQueue(Path file, boolean checkCurrent) {
    Preconditions.checkNotNull(file, "file cannot be null");
    if (checkCurrent) {
      try {
        boolean valid = StringUtils.isEmpty(currentFile.toString()) || compare(currentFile, file) < 0;
        if (!valid) {
          LOG.warn("File cannot be added to the queue: " + file.toString());
        }
      } catch (NoSuchFileException ex) {
        // Happens only in timestamp ordering.
        // Very unlikely this will happen, new file has to be added to the queue at the exact time when
        // the currentFile was consumed and archived while a new file has not yet been picked up for processing.
        // Ignore - we just add the new file, since this means this file is indeed newer
        // (else this would have been consumed and archived first)
      }
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

  private boolean canPoolFiles() {
    if(waitForPathAppearance) {
      try {
        DirectoryPathCreationWatcher watcher = new DirectoryPathCreationWatcher(Arrays.asList(spoolDirPath), 0);
        if (!watcher.find().isEmpty()) {
          waitForPathAppearance = false;
          startSpooling(this.currentFile);
        } else {
          LOG.debug(Utils.format("Directory Paths does not exist yet: {}", spoolDirPath));
        }
      } catch (IOException e) {
        throw new RuntimeException(Utils.format("Some Problem with the file system: {}", e.toString()), e);
      }
    }
    return !waitForPathAppearance;
  }

  public synchronized File poolForFile(long wait, TimeUnit timeUnit) throws InterruptedException {
    Preconditions.checkArgument(wait >= 0, "wait must be zero or greater");
    Preconditions.checkNotNull(timeUnit, "timeUnit cannot be null");

    if (!canPoolFiles()) {
      return null;
    }

    Preconditions.checkState(running, "Spool directory watcher not running");
    synchronized (this) {
      if (previousFile != null && !context.isPreview()) {
        switch (postProcessing) {
          case NONE:
            LOG.debug("Previous file '{}' remains in spool directory", previousFile);
            break;
          case DELETE:
            try {
              if (Files.exists(previousFile)) {
                LOG.debug("Deleting previous file '{}'", previousFile);
                Files.delete(previousFile);
              } else {
                LOG.error("failed to delete previous file '{}'", previousFile);
              }
            } catch (IOException ex) {
              throw new RuntimeException(Utils.format("Could not delete file '{}', {}", previousFile, ex.toString()),
                  ex);
            }
            break;
          case ARCHIVE:
            try {
              if (Files.exists(previousFile)) {
                LOG.debug("Archiving previous file '{}'", previousFile);
                moveIt(previousFile, archiveDirPath);
              } else {
                LOG.error("failed to Archive previous file '{}'", previousFile);
              }
            } catch (IOException ex) {
              throw new RuntimeException(Utils.format("Could not move file '{}' to archive dir {}, {}", previousFile,
                  archiveDirPath, ex.toString()), ex);
            }
            break;
          default:
            LOG.error("poolForFile(): switch failed. postProcesing " + postProcessing.name() + " " + postProcessing.toString());
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
        currentFile = next;
        previousFile = next;
      }
    }
    pendingFilesCounter.inc(filesQueue.size() - pendingFilesCounter.getCount());
    return (next != null) ? next.toFile() : null;
  }

  public void handleCurrentFileAsError() throws IOException {
    if (errorArchiveDirPath != null && !context.isPreview()) {
      Path current = spoolDirPath.resolve(previousFile);
      LOG.error("Archiving file in error '{}' in error archive directory '{}'", previousFile, errorArchiveDirPath);
      moveIt(current, errorArchiveDirPath);
      // we need to set the currentFile to null because we just moved to error.
      previousFile = null;
    } else {
      LOG.error("Leaving file in error '{}' in spool directory", currentFile);
    }
  }

  // This method is a simple wrapper that lets us find the NoSuchFileException if that was the cause.
  private int compare(Path path1, Path path2) throws NoSuchFileException {
    // why not just check if the file exists? Well, there is a possibility file gets moved/archived/deleted right after
    // that check. In that case we will still fail. So fail, and recover.
    try {
      return pathComparator.compare(path1, path2);
    } catch (RuntimeException ex) {
      Throwable cause = ex.getCause();
      if (cause != null && cause instanceof NoSuchFileException) {
        LOG.debug("Starting file may have already been archived.", cause);
        throw (NoSuchFileException) cause;
      }
      LOG.warn("Error while comparing files", ex);
      throw ex;
    }
  }

  private void moveIt(Path file, Path destinationRoot) throws IOException {
    // wipe out base of the path - leave subdirectory portion in place.
    String f = file.toString().replaceFirst(spoolDirPath.toString(), "");
    Path dest = Paths.get(destinationRoot.toString(), f);
    if(!file.equals(dest)) {
      dest.toFile().getParentFile().mkdirs();
      try {
        if (Files.exists(dest)) {
          Files.delete(dest);
        }
        Files.move(file, dest);
      } catch (Exception ex) {
        throw new IOException("moveIt: Files.delete or Files.move failed.  " + file + " " + dest + " " + ex.getMessage() + " destRoot " + destinationRoot);

      }
    }
  }

  private List<Path> findAndQueueFiles(
      final Path startingFile, final boolean includeStartingFile, boolean checkCurrent
  ) throws IOException {
    final long scanTime = System.currentTimeMillis();
    DirectoryStream.Filter<Path> filter = new DirectoryStream.Filter<Path>() {
      @Override
      public boolean accept(Path entry) throws IOException {
        boolean accept = false;
        // SDC-3551: Pick up only files with mtime strictly less than scan time.
        if (entry != null && Files.getLastModifiedTime(entry).toMillis() < scanTime && fileMatcher.matches(entry.getFileName())) {
          if (startingFile == null || startingFile.toString().isEmpty()) {
            accept = true;
          } else {
            try {
              int compares = compare(entry, startingFile);
              accept = (compares == 0 && includeStartingFile) || (compares > 0);
            } catch (NoSuchFileException ex) {
              // This happens only if timestamp is used, when the mtime is looked up for the startingFile
              // which has been archived, so this file must be newer since it is still in the directory
              // (if it was older it would have been consumed and archived earlier)
              return true;
            }
          }
        }
        return accept;
      }
    };


    final List<Path> directories = new ArrayList<>();

    if (processSubdirectories && useLastModified) {
      EnumSet<FileVisitOption> opts = EnumSet.noneOf(FileVisitOption.class);
      try {
        Files.walkFileTree(spoolDirPath, opts, Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult preVisitDirectory(
              Path dirPath, BasicFileAttributes attributes
          ) throws IOException {
            directories.add(dirPath);
            return FileVisitResult.CONTINUE;
          }
        });
      } catch (Exception ex) {
        throw new IOException("findAndQueueFiles(): walkFileTree error. startingFile " + startingFile + ex.getMessage(), ex);
      }
    } else {
      directories.add(spoolDirPath);
    }

    List<Path> foundFiles = new ArrayList<>(maxSpoolFiles);
    for (Path dir : directories) {
      try (DirectoryStream<Path> matchingFile = Files.newDirectoryStream(dir, filter)) {
        for (Path file : matchingFile) {
          if (!running) {
            return null;
          }
          if (Files.isDirectory(file)) {
            continue;
          }
          LOG.trace("Found file '{}'", file);
          foundFiles.add(file);
          if (foundFiles.size() > maxSpoolFiles) {
            throw new IllegalStateException(Utils.format("Exceeded max number '{}' of spool files in directory",
                maxSpoolFiles
            ));
          }
        }
      } catch(Exception ex) {
        LOG.error("findAndQueueFiles(): newDirectoryStream failed. " + ex.getMessage(), ex);
      }
    }

    if (!useLastModified) { // Sorted in the queue, if useLastModified is true.
      Collections.sort(foundFiles);
    }
    for (Path file : foundFiles) {
      addFileToQueue(file, checkCurrent);
      if (filesQueue.size() > maxSpoolFiles) {
        throw new IllegalStateException(Utils.format("Exceeded max number '{}' of spool files in directory",
            maxSpoolFiles
        ));
      }
    }
    spoolQueueMeter.mark(filesQueue.size());
    pendingFilesCounter.inc(filesQueue.size() - pendingFilesCounter.getCount());
    LOG.debug("Found '{}' files", filesQueue.size());
    return directories;
  }

  void handleOlderFiles(final Path startingFile) throws IOException {
    if (postProcessing != FilePostProcessing.NONE) {
      final ArrayList<Path> toProcess = new ArrayList<>();

      EnumSet<FileVisitOption> opts = EnumSet.noneOf(FileVisitOption.class);
      try {
        Files.walkFileTree(spoolDirPath, opts, Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(
              Path dirPath, BasicFileAttributes attributes
          ) throws IOException {

            if (compare(dirPath, startingFile) < 0) {
              toProcess.add(dirPath);
            }

            return FileVisitResult.CONTINUE;
          }
        });
      } catch (Exception ex) {
        throw new IOException("traverseDirectories(): walkFileTree error. startingFile "
            + startingFile
            + ex.getMessage(),
            ex
        );
      }

      for (Path p : toProcess) {
        switch (postProcessing) {
          case DELETE:
            if (fileMatcher.matches(p.getFileName())) {
              if (Files.exists(p)) {
                Files.delete(p);
                LOG.debug("Deleting old file '{}'", p);
              } else {
                LOG.debug("The old file '{}' does not exist", p);
              }
            } else {
              LOG.debug("Ignoring old file '{}' that do not match the file name pattern '{}'", p, pattern);
            }
            break;
          case ARCHIVE:
            if (fileMatcher.matches(p.getFileName())) {
              if (Files.exists(p)) {
                moveIt(p, archiveDirPath);
                LOG.debug("Archiving old file '{}'", p);
              } else {
                LOG.debug("The old file '{}' does not exist", p);
              }
            } else {
              LOG.debug("Ignoring old file '{}' that do not match the file name pattern '{}'", p, pattern);
            }
            break;
          case NONE:
            // no action required
            break;
          default:
            throw new IllegalStateException("Unexpected post processing option " + postProcessing);
        }
      }
    }
  }

  class FileFinder implements Runnable {

    public FileFinder(){
    }

    @Override
    public synchronized void run() {
      // by using current we give a chance to have unprocessed files out of order
      LOG.debug("Starting file finder from '{}'", currentFile);
      try {
        findAndQueueFiles(currentFile, false, true);
      } catch (Exception ex) {
        LOG.warn("Error while scanning directory '{}' for files newer than '{}': {}", archiveDirPath, currentFile,
            ex.toString(), ex);
      }
    }
  }

  class FilePurger implements Runnable {

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
      LOG.debug("Starting archived files purging");
      final long timeThreshold = System.currentTimeMillis() - archiveRetentionMillis;
      final ArrayList<Path> toProcess = new ArrayList<>();
      EnumSet<FileVisitOption> opts = EnumSet.noneOf(FileVisitOption.class);
      int purged = 0;
      try {
        Files.walkFileTree(archiveDirPath, opts, Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(
              Path entry, BasicFileAttributes attributes
          ) throws IOException {
            if (fileMatcher.matches(entry.getFileName()) && (
                timeThreshold - Files.getLastModifiedTime(entry).toMillis() > 0
            )) {
              toProcess.add(entry);
            }
            return FileVisitResult.CONTINUE;
          }
        });

        for (Path file : toProcess) {
          if (running) {
            LOG.debug("Deleting archived file '{}', exceeded retention time", file);
            try {
              if(Files.exists(file)) {
                Files.delete(file);
                purged++;
              }
            } catch (IOException ex) {
              LOG.warn("Error while deleting file '{}': {}", file, ex.toString(), ex);
            }
          } else {
            LOG.debug("Spooler has been destroyed, stopping archived files purging half way");
            break;
          }
        }
      } catch (IOException ex) {
        LOG.warn("Error while scanning directory '{}' for archived files purging: {}", archiveDirPath, ex.toString(),
            ex
        );
      }
      LOG.debug("Finished archived files purging, deleted '{}' files", purged);
    }
  }

}
