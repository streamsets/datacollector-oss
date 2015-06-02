/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.PostProcessingOptions;
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

  public FileContext(MultiFileInfo multiFileInfo, Charset charset, int maxLineLength,
      PostProcessingOptions postProcessing, String archiveDir, FileEventPublisher eventPublisher) throws IOException {
    this.multiFileInfo = multiFileInfo;
    this.charset = charset;
    this.maxLineLength = maxLineLength;
    this.postProcessing = postProcessing;
    this.archiveDir = archiveDir;
    this.eventPublisher = eventPublisher;
    Path fullPath = Paths.get(multiFileInfo.getFileFullPath());
    Path dir = fullPath.getParent();
    Path name = fullPath.getFileName();
    rollMode = multiFileInfo.getFileRollMode().createRollMode(name.toString(), multiFileInfo.getPattern());
    scanner = new LiveDirectoryScanner(dir.toString(), multiFileInfo.getFirstFile(), getRollMode());
  }

  public boolean hasReader() {
    return reader != null;
  }

  // prepares and gets the reader if available before a read.
  public LiveFileReader getReader() throws IOException {
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
        reader = new LiveFileReader(getRollMode(), getMultiFileInfo().getTag(), currentFile, charset, fileOffset,
                                    maxLineLength);
        if (fileOffset == 0) {
          // file start event
          eventPublisher.publish(new FileEvent(currentFile, true));
        }
      }
    }
    return reader;
  }

  // updates reader and offsets after a read.
  public void releaseReader() throws IOException {
    // update starting offsets for next invocation either cold (no reader) or hot (reader)
    if (!getReader().hasNext()) {
      // reached EOF
      getReader().close();
      reader = null;
      //using Long.MAX_VALUE to signal we reach the end of the file and next iteration should get the next file.
      setStartingCurrentFileName(currentFile);
      setStartingOffset(Long.MAX_VALUE);

      // file end event
      LiveFile file = currentFile.refresh();
      eventPublisher.publish(new FileEvent(file, false));
      switch (postProcessing) {
        case NONE:
          LOG.debug("File '{}' processing completed, post processing action 'NONE'", file);
          break;
        case DELETE:
          Files.delete(file.getPath());
          LOG.debug("File '{}' processing completed, post processing action 'DELETED'", file);
          break;
        case ARCHIVE:
          Path fileArchive = Paths.get(archiveDir, file.getPath().toString());
          try {
            Files.createDirectories(fileArchive.getParent());
            Files.move(file.getPath(), fileArchive);
            LOG.debug("File '{}' processing completed, post processing action 'ARCHIVED' as", file);
          } catch (IOException ex) {
            throw new IOException(Utils.format("Could not archive '{}': {}", file, ex.getMessage()), ex);
          }
          break;
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
    return startingCurrentFileName;
  }

  public long getStartingOffset() {
    return startingOffset;
  }

  public RollMode getRollMode() {
    return rollMode;
  }

  public void setStartingCurrentFileName(LiveFile startingCurrentFileName) {
    this.startingCurrentFileName = startingCurrentFileName;
  }

  public void setStartingOffset(long startingOffset) {
    this.startingOffset = startingOffset;
  }
}
