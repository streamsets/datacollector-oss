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

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.parser.shaded.com.google.code.regexp.Pattern;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * The <code>FileContext</code> encapsulates all live information about a directory being scanned/read.
 */
public class FileContext {
  private static final Logger LOG = LoggerFactory.getLogger(FileContext.class);
  private final MultiFileInfo multiFileInfo;
  private final LiveDirectoryScanner scanner;
  private final Charset charset;
  private final int maxLineLength;
  private final PostProcessingOptions postProcessing;
  private final String archiveDir;
  private final FileEventPublisher eventPublisher;
  private LiveFile currentFile;
  private LiveFileReader reader;
  private LiveFile startingCurrentFileName;
  private long startingOffset;
  private RollMode rollMode;
  private final Path dir;
  private boolean open;
  private boolean inPreviewMode;

  public FileContext(MultiFileInfo multiFileInfo, Charset charset, int maxLineLength,
      PostProcessingOptions postProcessing, String archiveDir, FileEventPublisher eventPublisher,
      boolean inPreviewMode) throws IOException {
    open = true;
    this.multiFileInfo = multiFileInfo;
    this.charset = charset;
    this.maxLineLength = maxLineLength;
    this.postProcessing = postProcessing;
    this.archiveDir = archiveDir;
    this.eventPublisher = eventPublisher;
    Path fullPath = Paths.get(multiFileInfo.getFileFullPath());
    dir = fullPath.getParent();
    Path name = fullPath.getFileName();
    rollMode = multiFileInfo.getFileRollMode().createRollMode(name.toString(), multiFileInfo.getPattern());
    scanner = new LiveDirectoryScanner(dir.toString(), multiFileInfo.getFirstFile(), getRollMode());
    this.inPreviewMode = inPreviewMode;
  }

  public String toString() {
    return Utils.format("FileContext[path={} rollMode={}]", multiFileInfo.getFileFullPath(), rollMode);
  }

  public long getPendingFiles() throws IOException{
    return scanner.getPendingFiles(currentFile);
  }

  public boolean hasReader() {
    Utils.checkState(open, "FileContext is closed");
    return reader != null;
  }

  // a file context is active while its parent directory exists.
  public boolean isActive() {
    return Files.exists(dir);
  }

  public void close() {
    if (open && reader != null) {
      open = false;
      try {
        reader.close();
      } catch (IOException ex) {
        LOG.warn("Could not close '{}' file property: {}", reader.getLiveFile(), ex.toString(), ex);
      } finally {
        reader = null;
      }
    }
  }

  // prepares and gets the reader if available before a read.
  public LiveFileReader getReader() throws IOException {
    Utils.checkState(open, "FileContext is closed");
    if (reader == null) {
      currentFile = getStartingCurrentFileName();
      long fileOffset = getStartingOffset();

      boolean needsToScan = currentFile == null || fileOffset == Long.MAX_VALUE;
      if (needsToScan) {
        if (currentFile != null) {
          // we need to refresh the file in case the name changed before scanning as the scanner does not refresh
          currentFile = currentFile.refresh();
        }
        currentFile = scanner.scan(currentFile);
        fileOffset = 0;
      }
      if (currentFile != null) {
        reader = new SingleLineLiveFileReader(getRollMode(), getMultiFileInfo().getTag(), currentFile, charset,
                                              fileOffset, maxLineLength);
        if (!multiFileInfo.getMultiLineMainLinePatter().isEmpty()) {
          reader = new MultiLineLiveFileReader(getMultiFileInfo().getTag(), reader,
                                               Pattern.compile(multiFileInfo.getMultiLineMainLinePatter()));
        }
        if (fileOffset == 0) {
          // file start event
          eventPublisher.publish(new FileEvent(currentFile, FileEvent.Action.START));
        }
      }
    }
    return reader;
  }

  // updates reader and offsets after a read.
  public void releaseReader(boolean inErrorDiscardReader) throws IOException {
    Utils.checkState(open, "FileContext is closed");
    // update starting offsets for next invocation either cold (no reader) or hot (reader)
    boolean hasNext;
    try {
      hasNext = reader != null && reader.hasNext();
    } catch (IOException ex) {
      IOUtils.closeQuietly(reader);
      reader = null;
      hasNext = false;
    }
    boolean doneWithFile = !hasNext || inErrorDiscardReader;
    if (doneWithFile) {
      IOUtils.closeQuietly(reader);
      reader = null;
      // Using Long.MAX_VALUE to signal we reach the end of the file and next iteration should get the next file.
      setStartingCurrentFileName(currentFile);
      setStartingOffset(Long.MAX_VALUE);

      // If we failed to open the file in first place, it will be null and hence we won't do anything with it.
      if(currentFile == null) {
        return;
      }

      // File end event
      LiveFile file = currentFile.refresh();

      if (inErrorDiscardReader) {
        LOG.warn("Processing file '{}' produced an error, skipping '{}' post processing on that file",
                 file, postProcessing);
        eventPublisher.publish(new FileEvent(file, FileEvent.Action.ERROR));
      } else {
        eventPublisher.publish(new FileEvent(file, FileEvent.Action.END));
        switch (postProcessing) {
          case NONE:
            LOG.debug("File '{}' processing completed, post processing action 'NONE'", file);
            break;
          case DELETE:
            if(!inPreviewMode) {
              try {
                Files.delete(file.getPath());
                LOG.debug("File '{}' processing completed, post processing action 'DELETED'", file);
              } catch (IOException ex) {
                throw new IOException(Utils.format("Could not delete '{}': {}", file, ex.toString()), ex);
              }
            }
            break;
          case ARCHIVE:
            if(!inPreviewMode) {
              Path fileArchive = Paths.get(archiveDir, file.getPath().toString());
              if (fileArchive == null) {
                throw new IOException("Could not find archive file");
              }
              try {
                Files.createDirectories(fileArchive.getParent());
                Files.move(file.getPath(), fileArchive);
                LOG.debug("File '{}' processing completed, post processing action 'ARCHIVED' as", file);
              } catch (IOException ex) {
                throw new IOException(Utils.format("Could not archive '{}': {}", file, ex.toString()), ex);
              }
            }
            break;
        }
      }
    } else {
      setStartingCurrentFileName(currentFile);
      setStartingOffset(getReader().getOffset());
    }
  }

  public MultiFileInfo getMultiFileInfo() {
    return multiFileInfo;
  }

  public LiveFile getStartingCurrentFileName() {
    Utils.checkState(open, "FileContext is closed");
    return startingCurrentFileName;
  }

  public long getStartingOffset() {
    Utils.checkState(open, "FileContext is closed");
    return startingOffset;
  }

  public RollMode getRollMode() {
    Utils.checkState(open, "FileContext is closed");
    return rollMode;
  }

  public void setStartingCurrentFileName(LiveFile startingCurrentFileName) {
    Utils.checkState(open, "FileContext is closed");
    this.startingCurrentFileName = startingCurrentFileName;
  }

  public void setStartingOffset(long startingOffset) {
    Utils.checkState(open, "FileContext is closed");
    this.startingOffset = startingOffset;
  }
}
