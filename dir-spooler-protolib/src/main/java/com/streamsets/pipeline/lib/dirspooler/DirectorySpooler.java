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
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DirectorySpooler {
  private static final Logger LOG = LoggerFactory.getLogger(DirectorySpooler.class);
  private static final String PENDING_FILES = "pending.files";

  private final PushSource.Context context;
  private final String spoolDir;
  protected final int maxSpoolFiles;
  private final String pattern;
  private final PathMatcherMode pathMatcherMode;
  private final FilePostProcessing postProcessing;
  private final String archiveDir;
  private final long archiveRetentionMillis;
  private final String errorArchiveDir;
  protected final boolean useLastModified;
  private final Comparator<WrappedFile> pathComparator;
  protected final boolean processSubdirectories;
  private final long spoolingPeriodSec;
  protected final WrappedFileSystem fs;
  protected final ReadWriteLock closeLock = new ReentrantReadWriteLock();

  private final long intervalMillis;
  private Exception destroyCauseException;

  public enum FilePostProcessing {NONE, DELETE, ARCHIVE}

  public static class Builder {
    protected PushSource.Context context;
    protected String spoolDir;
    protected int maxSpoolFiles;
    protected String pattern;
    protected PathMatcherMode pathMatcherMode = PathMatcherMode.GLOB;
    protected FilePostProcessing postProcessing;
    protected String archiveDir;
    protected long archiveRetentionMillis;
    protected String errorArchiveDir;
    protected boolean waitForPathAppearance;
    protected boolean useLastModifiedTimestamp;
    protected boolean processSubdirectories;
    protected long spoolingPeriodSec = 5;
    protected WrappedFileSystem fs;

    public Builder() {
      postProcessing = FilePostProcessing.NONE;
    }

    public Builder setContext(PushSource.Context context) {
      this.context = Preconditions.checkNotNull(context, "context cannot be null");
      return this;
    }

    public Builder setDir(String dir) throws IOException {
      Preconditions.checkNotNull(fs, "WrappedFileSystem cannot be null, setWrappedFileSystem before setDir");
      final String spoolDirInput = Preconditions.checkNotNull(dir, "dir cannot be null");
      final WrappedFile spoolDirPath = fs.getFile(spoolDirInput);
      Preconditions.checkArgument(spoolDirPath.isAbsolute(), Utils.formatL("dir '{}' must be an absolute path", dir));
      // normalize path to ensure no trailing slash
      spoolDir = spoolDirPath.getAbsolutePath();
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

    public Builder setSpoolingPeriodSec(long spoolingPeriodSec) {
      this.spoolingPeriodSec = spoolingPeriodSec;
      return this;
    }

    public Builder setWrappedFileSystem(WrappedFileSystem fs) {
      this.fs = fs;
      return this;
    }

    /**
     * Check builder attributes are correctly set to properly build a DirectorySpooler through the
     * {@link Builder#build()} method.
     */
    protected void checkArguments() {
      Preconditions.checkArgument(context != null, "context not specified");
      Preconditions.checkArgument(spoolDir != null, "spool dir not specified");
      Preconditions.checkArgument(maxSpoolFiles > 0, "max spool files not specified");
      Preconditions.checkArgument(pattern != null, "file pattern not specified");
      if (postProcessing == FilePostProcessing.ARCHIVE) {
        Preconditions.checkArgument(archiveDir != null, "archive dir not specified");
      }
    }

    public DirectorySpooler build() {
      checkArguments();
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
          processSubdirectories,
          spoolingPeriodSec,
          fs
      );
    }
  }

  // used for unit testing
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
        processSubdirectories,
        5,
        null
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
      boolean processSubdirectories,
      long spoolingPeriodSec,
      WrappedFileSystem fs
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
    this.spoolingPeriodSec = spoolingPeriodSec;
    this.fs = fs;

    pathComparator = fs.getComparator(useLastModified);
    intervalMillis = 5000;
  }

  protected volatile WrappedFile currentFile;
  protected WrappedFile initialFile;

  protected WrappedFile spoolDirPath;
  private WrappedFile archiveDirPath;
  private WrappedFile errorArchiveDirPath;
  protected PriorityBlockingQueue<WrappedFile> filesQueue;
  private ScheduledExecutorService scheduledExecutor;
  private boolean waitForPathAppearance;

  protected Meter spoolQueueMeter;
  protected Counter pendingFilesCounter;

  protected volatile boolean running;

  volatile FilePurger purger;
  volatile FileFinder finder;

  private Set<WrappedFile> filesBeingProcessed;

  private void checkBaseDir(WrappedFile path) throws IOException {
    Preconditions.checkState(path.isAbsolute(), Utils.formatL("Path '{}' is not an absolute path", path));

    if (SpoolDirUtil.isGlobPattern(path.getAbsolutePath())) {
      String absolutePath = SpoolDirUtil.truncateGlobPatternDirectory(path.getAbsolutePath());
      Preconditions.checkState(fs.exists(fs.getFile(absolutePath)), Utils.formatL("Path '{}' does not exist", path));
      Preconditions.checkState(fs.isDirectory(fs.getFile(absolutePath)), Utils.formatL("Path '{}' is not a directory", path));
    } else {
      Preconditions.checkState(fs.exists(path), Utils.formatL("Path '{}' does not exist", path));
      Preconditions.checkState(fs.isDirectory(path), Utils.formatL("Path '{}' is not a directory", path));
    }
  }

  public void init(String sourceFile) {
    try {
      spoolDirPath = fs.getFile(spoolDir);

      // 11 is the DEFAULT_INITIAL_CAPACITY -- seems pretty random, but lets use the same one.
      filesQueue = new PriorityBlockingQueue<>(11, pathComparator);
      filesBeingProcessed = ConcurrentHashMap.newKeySet();

      if(StringUtils.isEmpty(sourceFile)) {
        this.currentFile = null;
      } else {
        // sourceFile can contain: a filename, a partial path (relative to spoolDirPath),
        // or a full path.
        this.currentFile = fs.getFile(spoolDir, sourceFile);
        if (this.currentFile.getParent() == null
            || !(this.currentFile.getParent().toString().contains(spoolDirPath.toString()))) {
          // if filename only or not full path - add the full path to the filename
          this.currentFile = fs.getFile(spoolDirPath.toString(), sourceFile);
        }
        this.initialFile = this.currentFile;
        // Adding initialFile to the filesQueue as it is not added later due to thread safety
        if (fs.exists(initialFile)) {
          filesQueue.add(initialFile);
        }
      }

      if (!waitForPathAppearance) {
        checkBaseDir(spoolDirPath);
      }
      if (postProcessing == FilePostProcessing.ARCHIVE) {
        archiveDirPath = fs.getFile(archiveDir);
        checkBaseDir(archiveDirPath);
      }
      if (errorArchiveDir != null) {
        errorArchiveDirPath = fs.getFile(errorArchiveDir);
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

      spoolQueueMeter = context.createMeter("spoolQueue");

      pendingFilesCounter = context.createCounter(PENDING_FILES);

      if (!waitForPathAppearance) {
        startSpooling();
      }

    } catch (IOException ex) {
      destroy();
      throw new RuntimeException(ex);
    }
  }

  private void startSpooling() throws IOException {
    running = true;

    scheduledExecutor = new SafeScheduledExecutorService(1, "directory-dirspooler");

    findAndQueueFiles(true, false);

    finder = new FileFinder();
    scheduledExecutor.scheduleAtFixedRate(finder, spoolingPeriodSec, spoolingPeriodSec, TimeUnit.SECONDS);

    if (postProcessing == FilePostProcessing.ARCHIVE && archiveRetentionMillis > 0) {
      // create and schedule file purger only if the retention time is > 0
      purger = new FilePurger();
      scheduledExecutor.scheduleAtFixedRate(purger, 1, 1, TimeUnit.MINUTES);
    }
  }

  public void destroy(Exception causeDestroy){
    destroyCauseException = causeDestroy;
    destroy();
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

  public void addFileBeingProcessed(WrappedFile file) {
    if (file != null && !filesBeingProcessed.contains(file)) {
      filesBeingProcessed.add(file);
    }
  }

  public void removeFileBeingProcessed(WrappedFile file) {
    if (file != null) {
      filesBeingProcessed.remove(file);
    }
  }

  public void doPostProcessing(WrappedFile file) {
    switch (postProcessing) {
      case NONE:
        LOG.debug("Previous file '{}' remains in spool directory", file);
        break;
      case DELETE:
        try {
          if (fs.exists(file)) {
            LOG.debug("Deleting file '{}'", file);
            fs.delete(file);
          } else {
            LOG.error("failed to delete file '{}'", file);
          }
        } catch (IOException ex) {
          throw new RuntimeException(Utils.format("Could not delete file '{}', {}", file, ex.toString()),
              ex);
        }
        break;
      case ARCHIVE:
        try {
          if (fs.exists(file)) {
            LOG.debug("Archiving file '{}'", file);
            moveIt(file, archiveDirPath);
          } else {
            LOG.error("failed to Archive file '{}'", file);
          }
        } catch (IOException ex) {
          throw new RuntimeException(Utils.format("Could not move file '{}' to archive dir {}, {}", file,
              archiveDirPath, ex.toString()), ex);
        }
        break;
      default:
        LOG.error("poolForFile(): switch failed. postProcesing " + postProcessing.name() + " " + postProcessing.toString());
    }
    removeFileBeingProcessed(file);
  }

  protected void addFileToQueue(WrappedFile file, boolean checkCurrent) {
    Preconditions.checkNotNull(file, "file cannot be null");
    if (checkCurrent) {
      final boolean currentFileExists = currentFile != null && StringUtils.isNotEmpty(currentFile.toString());
      /* param "file" is invalid to add to queue if it is "less" than current file, in this case, useLastModified */
      final boolean invalid = currentFileExists && (fs.compare(file, currentFile, useLastModified) < 0);
      if (invalid) {
        String invalidReason;
        if (useLastModified) {
          final Map<String, Object> currentFileMetadata = currentFile != null
              ? currentFile.getCustomMetadata()
              : Collections.emptyMap();
          final Map<String, Object> newFileMetadata = file.getCustomMetadata();

          final long currentMtime = (long) currentFileMetadata.getOrDefault(
              HeaderAttributeConstants.LAST_MODIFIED_TIME,
              -1l
          );
          final long currentCtime = (long) currentFileMetadata.getOrDefault(
              HeaderAttributeConstants.LAST_CHANGE_TIME,
              -1l
          );
          final long newMtime = (long) newFileMetadata.getOrDefault(
              HeaderAttributeConstants.LAST_MODIFIED_TIME,
              -1l
          );
          final long newCtime = (long) newFileMetadata.getOrDefault(
              HeaderAttributeConstants.LAST_CHANGE_TIME,
              -1l
          );
          invalidReason = String.format(
              "it is older than the current file (which is '%s' with %s %d, %s %d): %s %d, %s %d",
              currentFile.getAbsolutePath(),
              HeaderAttributeConstants.LAST_MODIFIED_TIME,
              currentMtime,
              HeaderAttributeConstants.LAST_CHANGE_TIME,
              currentCtime,
              HeaderAttributeConstants.LAST_MODIFIED_TIME,
              newMtime,
              HeaderAttributeConstants.LAST_CHANGE_TIME,
              newCtime
          );
        } else {
          invalidReason = String.format(
              "it is earlier lexicographically than the current file (%s)",
              currentFile.getAbsolutePath()
          );
        }
        LOG.warn("File '{}' cannot be added to the queue; reason: {}", file.getAbsolutePath(), invalidReason);
        // allow control to flow through anyway (so file gets added to queue), since that has been the behavior forever
      }
    }

    if (!filesQueue.contains(file) && !filesBeingProcessed.contains(file)) {
      if (currentFile != null) {
        if (fs.compare(file, currentFile, useLastModified) > 0) {
          filesQueue.add(file);
        }
      } else {
        filesQueue.add(file);
      }
      spoolQueueMeter.mark(filesQueue.size());
    } else {
      LOG.debug("File '{}' already in queue, ignoring", file);
    }
  }

  private boolean canPoolFiles() {
    if (waitForPathAppearance) {
      try {
        if (fs.findDirectoryPathCreationWatcher(Arrays.asList(spoolDirPath))) {
          waitForPathAppearance = false;
          startSpooling();
        }
      } catch (IOException e) {
        throw new RuntimeException(Utils.format("Some Problem with the file system: {}", e.toString()), e);
      }
    }
    return !waitForPathAppearance;
  }

  public synchronized WrappedFile poolForFile(long wait, TimeUnit timeUnit) throws InterruptedException {
    Preconditions.checkArgument(wait >= 0, "wait must be zero or greater");
    Preconditions.checkNotNull(timeUnit, "timeUnit cannot be null");
    long initial = System.currentTimeMillis();

    while (!context.isStopped() && System.currentTimeMillis() - initial < wait && !canPoolFiles()) {
      Thread.sleep(intervalMillis);
    }
    if (!canPoolFiles()) {
      return null;
    }

    Preconditions.checkState(running, "Spool directory findDirectoryPathCreationWatcher not running");

    WrappedFile next = null;

    LOG.debug("Polling for file, waiting '{}' ms", TimeUnit.MILLISECONDS.convert(wait, timeUnit));
    while (!context.isStopped() && System.currentTimeMillis() - initial < wait && next == null) {
      closeLock.readLock().lock();
      try {
        next = filesQueue.peek();
        if (next != null) {
          if (filesBeingProcessed.contains(next)) {
            // file is already being processed by some other thread
            next = null;
          } else {
            addFileBeingProcessed(next);
          }
        }
      } finally {
        LOG.debug("Polling for file returned '{}'", next);

        if (next != null) {
          currentFile = next;
        }

        filesQueue.poll();

        closeLock.readLock().unlock();
      }

      if (next == null) {
        Thread.sleep(intervalMillis);
      }
    }

    pendingFilesCounter.inc(filesQueue.size() - pendingFilesCounter.getCount());
    return next;
  }

  public void handleFileAsError(WrappedFile fileToHandle) throws IOException {
    if (errorArchiveDirPath != null && !context.isPreview()) {
      if (fs.exists(fileToHandle)) {
        LOG.error("Archiving file in error '{}' in error archive directory '{}'", fileToHandle, errorArchiveDirPath);
        moveIt(fileToHandle, errorArchiveDirPath);
      } else {
        LOG.error(
            "Wanted to archive file in error '{}' in error archive directory '{}' but file does not exist.",
            fileToHandle,
            errorArchiveDirPath
        );
      }
    } else {
      LOG.error("Leaving file in error '{}' in spool directory", fileToHandle);
    }
  }

  private void moveIt(WrappedFile file, WrappedFile destinationRoot) throws IOException {
    // wipe out base of the path - leave subdirectory portion in place.
    String f = file.toString().replaceFirst(spoolDirPath.toString(), "");
    WrappedFile dest = fs.getFile(destinationRoot.toString(), f);
    if(!file.equals(dest)) {
      fs.mkdirs(fs.getFile(dest.getParent()));

      try {
        if (fs.exists(dest)) {
          fs.delete(dest);
        }
        fs.move(file, dest);
      } catch (Exception ex) {
        throw new IOException(Utils.format("Can't move {} to {}", file, dest), ex);
      }
    }
  }

  protected void findAndQueueFiles(final boolean includeStartingFile, boolean checkCurrent)
      throws IOException {
    if (filesQueue.size() >= maxSpoolFiles) {
      LOG.debug(Utils.format("Exceeded max number '{}' of spool files in directory", maxSpoolFiles));
      return;
    }

    final List<WrappedFile> directories = new ArrayList<>();

    if (processSubdirectories && useLastModified) {
      try {
        fs.addDirectory(spoolDirPath, directories);
      } catch (Exception ex) {
        throw new IOException(
            String.format(
                "findAndQueueFiles(): walkFileTree error. currentFile: '%s' %n error message: %s",
                currentFile,
                ex.getMessage()
            ),
            ex
        );
      }
    } else {
      directories.add(spoolDirPath);
    }

    for (WrappedFile dir : directories) {
      try {
        List<WrappedFile> matchingFile = new ArrayList<>();

        fs.addFiles(dir, this.currentFile, matchingFile, includeStartingFile, useLastModified);

        if (matchingFile.size() > 0) {
          try {
            // if there are matching files, acquire write lock
            closeLock.writeLock().lock();

            for (WrappedFile file : matchingFile) {
              if (!running) {
                return;
              }

              if (this.currentFile == null
                  || (this.initialFile != null && fs.compare(this.currentFile, this.initialFile, useLastModified) == 0)
                  || fs.compare(file, this.currentFile, useLastModified) > 0) {
                if (!fs.isDirectory(file)) {
                  LOG.trace("Found file '{}'", file);
                  addFileToQueue(file, checkCurrent);
                }
              } else {
                LOG.trace("Discarding file {} because it is already older than currentFile", file.getAbsolutePath());
              }
            }
          } finally {
            closeLock.writeLock().unlock();
          }
        }

      } catch(IOException ex) {
        LOG.error("findAndQueueFiles(): newDirectoryStream failed. " + ex.getMessage(), ex);
        destroy(ex);
      }
    }

    spoolQueueMeter.mark(filesQueue.size());
    pendingFilesCounter.inc(filesQueue.size() - pendingFilesCounter.getCount());
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
        findAndQueueFiles(false, true);
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
      final ArrayList<WrappedFile> toProcess = new ArrayList<>();
      int purged = 0;
      try {
        fs.archiveFiles(archiveDirPath, toProcess, timeThreshold);

        for (WrappedFile file : toProcess) {
          if (running) {
            LOG.debug("Deleting archived file '{}', exceeded retention time", file);
            try {
              if(fs.exists(file)) {
                fs.delete(file);
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

  public Exception getDestroyCause(){
    return destroyCauseException;
  }
}
