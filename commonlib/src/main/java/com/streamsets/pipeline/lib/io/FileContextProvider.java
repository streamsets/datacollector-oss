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

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FileContextProvider implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(FileContextProvider.class);

  private final List<FileContext> fileContexts;
  private final Set<String> fileKeys;
  private int startingIdx;
  private int currentIdx;
  private int loopIdx;

  public FileContextProvider(List<MultiFileInfo> fileInfos, Charset charset, int maxLineLength,
      PostProcessingOptions postProcessing, String archiveDir, FileEventPublisher eventPublisher) throws IOException {
    fileContexts = new ArrayList<>();
    fileKeys = new LinkedHashSet<>();
    for (MultiFileInfo dirInfo : fileInfos) {
      fileContexts.add(new FileContext(dirInfo, charset, maxLineLength, postProcessing, archiveDir, eventPublisher));
      if (fileKeys.contains(dirInfo.getFileKey())) {
        throw new IOException(Utils.format("File '{}' already specified, it cannot be added more than once",
                                           dirInfo.getFileKey()));
      }
      fileKeys.add(dirInfo.getFileKey());
    }
    LOG.debug("Opening files: {}", getFileKeys());
    startingIdx = 0;
  }

  private Set<String> getFileKeys() {
    return fileKeys;
  }

  /**
   * Sets the file offsets to use for the next read. To work correctly, the last return offsets should be used or
   * an empty <code>Map</code> if there is none.
   * <p/>
   * If a reader is already live, the corresponding set offset is ignored as we cache all the contextual information
   * of live readers.
   *
   * @param offsets directory offsets.
   * @throws IOException thrown if there was an IO error while preparing file offsets.
   */
  public void setOffsets(Map<String, String> offsets) throws IOException {
    Utils.checkNotNull(offsets, "offsets");
    // retrieve file:offset for each directory
    for (FileContext fileContext : fileContexts) {
      String offset = offsets.get(fileContext.getMultiFileInfo().getFileKey());
      LiveFile file = null;
      long fileOffset = 0;
      if (offset != null && !offset.isEmpty()) {
        String[] split = offset.split("::", 2);
        file = LiveFile.deserialize(split[1]).refresh();
        fileOffset = Long.parseLong(split[0]);
      }
      fileContext.setStartingCurrentFileName(file);
      fileContext.setStartingOffset(fileOffset);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Setting offset: directory '{}', file '{}', offset '{}'",
                  fileContext.getMultiFileInfo().getFileFullPath(), file, fileOffset);
      }
    }
    currentIdx = startingIdx;
  }

  /**
   * Returns the current file offsets. The returned offsets should be set before the next read.
   *
   * @return the current file offsets.
   * @throws IOException thrown if there was an IO error while preparing file offsets.
   */
  public Map<String, String> getOffsets() throws IOException {
    Map<String, String> map = new HashMap<>();
    // produce file:offset for each directory taking into account a current reader and its file state.
    for (FileContext fileContext : fileContexts) {
      LiveFile file;
      long fileOffset;
      if (!fileContext.hasReader()) {
        file = fileContext.getStartingCurrentFileName();
        fileOffset = fileContext.getStartingOffset();
      } else if (fileContext.getReader().hasNext()) {
        file = fileContext.getReader().getLiveFile();
        fileOffset = fileContext.getReader().getOffset();
      } else {
        file = fileContext.getReader().getLiveFile();
        fileOffset = Long.MAX_VALUE;
      }
      String offset = (file == null) ? "" : Long.toString(fileOffset) + "::" + file.serialize();
      map.put(fileContext.getMultiFileInfo().getFileKey(), offset);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Reporting offset: directory '{}', pattern: '{}', file '{}', offset '{}'",
                  fileContext.getMultiFileInfo().getFileFullPath(),
                  fileContext.getRollMode().getPattern(),
                  file,
                  fileOffset
        );
      }
    }
    startingIdx = getAndIncrementIdx();
    loopIdx = 0;
    return map;
  }

  public FileContext next() {
    loopIdx++;
    return fileContexts.get(getAndIncrementIdx());
  }

  public boolean didFullLoop() {
    return loopIdx >= fileContexts.size();
  }

  public void startNewLoop() {
    loopIdx = 0;
  }

  private int getAndIncrementIdx() {
    int idx = currentIdx;
    incrementIdx();
    return idx;
  }

  private int incrementIdx() {
    currentIdx = (currentIdx + 1) % fileContexts.size();
    return currentIdx;
  }

  @Override
  public void close() {
    LOG.debug("Closing files: {}", getFileKeys());
    for (FileContext fileContext : fileContexts) {
      LiveFile file = null;
      try {
        if (fileContext.hasReader()) {
          file = fileContext.getReader().getLiveFile();
          fileContext.getReader().close();
        }
      } catch (IOException ex) {
        LOG.warn("Could not close '{}': {}", file, ex.getMessage(), ex);
      }
    }
  }

}
