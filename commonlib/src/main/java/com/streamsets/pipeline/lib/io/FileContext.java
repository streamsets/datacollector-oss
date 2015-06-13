/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.parser.shaded.com.google.code.regexp.Pattern;
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

  public FileContext(MultiFileInfo multiFileInfo, Charset charset, int maxLineLength,
      PostProcessingOptions postProcessing, String archiveDir, FileEventPublisher eventPublisher) throws IOException {
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
  }

  public String toString() {
    return Utils.format("FileContext[path={} rollMode={}]", multiFileInfo.getFileFullPath(), rollMode);
  }

  public boolean hasReader() {
    Utils.checkState(open, "FileContext is closed");
    return reader != null;
  }

  // a file context is active while its parent directory exists.
  public boolean isActive() {
    Utils.checkState(open, "FileContext is closed");
    return Files.exists(dir);
  }

  public void close() throws IOException {
    if (open && reader != null) {
      open = false;
      try {
        reader.close();
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
          eventPublisher.publish(new FileEvent(currentFile, true));
        }
      }
    }
    return reader;
  }

  // updates reader and offsets after a read.
  public void releaseReader() throws IOException {
    Utils.checkState(open, "FileContext is closed");
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
    Utils.checkState(open, "FileContext is closed");
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
