/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.stage.origin.datalake;

import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.lib.dirspooler.DirectorySpooler;
import com.streamsets.pipeline.lib.dirspooler.PathMatcherMode;
import com.streamsets.pipeline.lib.dirspooler.WrappedFile;
import com.streamsets.pipeline.lib.dirspooler.WrappedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class AzureDirectorySpooler extends DirectorySpooler {
  private static final Logger LOG = LoggerFactory.getLogger(AzureDirectorySpooler.class);

  public static class Builder extends DirectorySpooler.Builder {
    public Builder() {
      super();
    }

    public DirectorySpooler build() {
      checkArguments();
      return new AzureDirectorySpooler(
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

  public AzureDirectorySpooler(
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
    super(
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
        useLastModified,
        processSubdirectories,
        spoolingPeriodSec,
        fs
    );
  }

  @Override
  protected void findAndQueueFiles(final boolean includeStartingFile, boolean checkCurrent) throws IOException {
    if (filesQueue.size() >= maxSpoolFiles) {
      LOG.warn("Exceeded max number '{}' of spool files in directory", maxSpoolFiles);
      return;
    }

    LOG.trace("Scanning directory for files and dirs...");
    List<WrappedFile> files = fs.walkDirectory(spoolDirPath, currentFile, includeStartingFile, useLastModified);
    LOG.trace("Found {} files during scanning...", files.size());

    for (WrappedFile file : files) {
      LOG.trace("findAndQueueFiles: '{}'", file.getAbsolutePath());
      if (!running) {
        return;
      }

      try {
        if (currentFile == null || (
            this.initialFile != null && fs.compare(this.currentFile, this.initialFile, useLastModified) == 0
        ) || fs.compare(file, this.currentFile, useLastModified) > 0) {
          if (!fs.isDirectory(file)) {
            closeLock.writeLock().lock();
            addFileToQueue(file, checkCurrent);
            closeLock.writeLock().unlock();
          }
        } else {
          LOG.trace("Discarding file {} because it is already older than currentFile", file.getAbsolutePath());
        }
      } catch (Exception ex) {
        LOG.error("findAndQueueFiles(): newDirectoryStream failed. " + ex.getMessage(), ex);
      }
    }

    spoolQueueMeter.mark(filesQueue.size());
    pendingFilesCounter.inc(filesQueue.size() - pendingFilesCounter.getCount());
    LOG.trace("Priority queue has '{}' files.", filesQueue.size());
  }

}
