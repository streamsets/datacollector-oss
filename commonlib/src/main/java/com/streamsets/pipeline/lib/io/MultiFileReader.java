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
import com.streamsets.pipeline.lib.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The <code>MultiFileReader</code> is a Reader that allows to read multiple files in a 'tail -f' mode while
 * keeping track of the current offsets and detecting if the files has been renamed.
 * <p/>
 * It builds on top of the {@link SingleLineLiveFileReader} adding support for reading data from multiple files in different
 * directories.
 * <p/>
 * Directories are read in round-robin fashion to avoid starvation.
 * <p/>
 * The usage pattern is:
 * <p/>
 * <pre>
 *   offsetMap = ....
 *   reader.setOffsets(offsetMap);
 *   chunk = reader.next(timeoutInMillis);
 *   if (chunk != null) {
 *     ....
 *   }
 *   offsetMap = reader.getOffsets();
 * </pre>
 * <p/>
 * The offsetMap must be kept/persisted by the caller to ensure current offsets are not lost.
 */
public class MultiFileReader implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(MultiFileReader.class);

  private final static long MAX_YIELD_TIME = Integer.parseInt(System.getProperty("MultiFileReader.yield.ms", "500"));

  private final FileContextProvider fileContextProvider;
  private final List<FileEvent> events;
  private boolean open;
  private boolean inPreviewMode;

  /**
   * Creates a <code>MultiFileReader</code> that will scan/read multiple directories for data.
   *
   * @param fileInfos a list with the information for for each directory to scan/read.
   * @param charset the data charset (for all files)
   * @param maxLineLength the maximum line length (for all files)
   * @throws IOException thrown if there was an IO error while creating the reader.
   */
  public MultiFileReader(
      List<MultiFileInfo> fileInfos,
      Charset charset,
      int maxLineLength,
      PostProcessingOptions postProcessing,
      String archiveDir,
      boolean globbing,
      int scanIntervalSecs,
      boolean allowForLateDirectoryCreation,
      boolean inPreviewMode
  ) throws IOException {
    Utils.checkNotNull(fileInfos, "fileInfos");
    Utils.checkArgument(!fileInfos.isEmpty(), "fileInfos cannot be empty");
    Utils.checkNotNull(charset, "charset");
    Utils.checkArgument(maxLineLength > 1, "maxLineLength must be greater than one");
    Utils.checkNotNull(postProcessing, "postProcessing");
    Utils.checkArgument(
        postProcessing != PostProcessingOptions.ARCHIVE || (archiveDir != null && !archiveDir.isEmpty()),
        "archiveDir cannot be empty if postProcessing is ARCHIVE");

    archiveDir = (postProcessing == PostProcessingOptions.ARCHIVE) ? archiveDir : null;
    this.inPreviewMode = inPreviewMode;

    events = new ArrayList<>(fileInfos.size() * 2);
    FileEventPublisher eventPublisher = new FileEventPublisher() {
      @Override
      public void publish(FileEvent event) {
        events.add(event);
      }
    };

    //We assume ExactFileContextProvider has fileInfo which are exact and present.
    //We are using GlobFileInfo during FileTailSource which will allow for late Directory creation/ supports wild cards.
    fileContextProvider = (globbing)? new GlobFileContextProvider(
        allowForLateDirectoryCreation,
        fileInfos,
        scanIntervalSecs,
        charset,
        maxLineLength,
        postProcessing,
        archiveDir,
        eventPublisher,
        inPreviewMode
    ) : new ExactFileContextProvider(
        fileInfos,
        charset,
        maxLineLength,
        postProcessing,
        archiveDir,
        eventPublisher,
        inPreviewMode
    );

    open = true;
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
    Utils.checkState(open, "Not open");
    fileContextProvider.setOffsets(offsets);
    // we reset the events on every setOffsets().
    events.clear();
  }

  /**
   * Purge invalid file entries.
   */
  public void purge() {
    fileContextProvider.purge();
  }

  /**
   * Returns the current file offsets. The returned offsets should be set before the next read.
   *
   * @return the current file offsets.
   * @throws IOException thrown if there was an IO error while preparing file offsets.
   */
  public Map<String, String> getOffsets() throws IOException {
    Utils.checkState(open, "Not open");
    return fileContextProvider.getOffsets();
  }

  /**
   * Returns all file events (start and end) since the last {@link #setOffsets(java.util.Map)} call.
   *
   * @return all files events.
   */
  public List<FileEvent> getEvents() {
    return events;
  }

  // if we are in timeout
  private boolean isTimeout(long startTime ,long maxWaitTimeMillis) {
    return (System.currentTimeMillis() - startTime) > maxWaitTimeMillis;
  }

  // remaining time till  timeout, return zero if already in timeout
  private long getRemainingWaitTime(long startTime, long maxWaitTimeMillis) {
    long remaining = maxWaitTimeMillis - (System.currentTimeMillis() - startTime);
    return (remaining > 0) ? remaining : 0;
  }

  /**
   * Reads the next {@link LiveFileChunk} from the directories waiting the specified time for one.
   *
   * @param waitMillis number of milliseconds to block waiting for a chunk.
   * @return the next chunk, or <code>null</code> if there is no next chunk and the waiting time passed.
   */
  public LiveFileChunk next(long waitMillis) {
    Utils.checkState(open, "Not open");
    waitMillis = (waitMillis > 0) ? waitMillis : 0;
    long startTime = System.currentTimeMillis();
    LiveFileChunk chunk = null;
    boolean exit = false;
    fileContextProvider.startNewLoop();
    while (!exit) {
      if (!fileContextProvider.didFullLoop()) {
        FileContext fileContext = fileContextProvider.next();
        try {
          LiveFileReader reader = fileContext.getReader();
          if (reader != null) {
            if (reader.hasNext()) {
              chunk = reader.next(0);
              if (LOG.isTraceEnabled()) {
                LOG.trace("next(): directory '{}', file '{}', offset '{}' got data '{}'",
                    fileContext.getMultiFileInfo().getFileFullPath(),
                    reader.getLiveFile(), reader.getOffset(), chunk != null);
              }
            } else {
              if (LOG.isTraceEnabled()) {
                LOG.trace("next(): directory '{}', file '{}', offset '{}' EOF reached",
                    fileContext.getMultiFileInfo().getFileFullPath(),
                    reader.getLiveFile(), reader.getOffset());
              }
            }
            fileContext.releaseReader(false);
          } else {
            if (LOG.isTraceEnabled()) {
              LOG.trace("next(): directory '{}', no reader available",
                  fileContext.getMultiFileInfo().getFileFullPath());
            }
          }
        } catch (IOException ex) {
          LOG.error("Error while reading file: {}", ex.toString(), ex);
          try {
            fileContext.releaseReader(true);
          } catch (IOException ex1) {
            LOG.warn("Error while releasing reader in error: {}", ex1.toString(), ex1);
          }
        }
      }

      // check exit conditions (we have a chunk, or we timed-out waitMillis)
      exit = chunk != null;
      if (!exit) {
        // if we looped thru all dir contexts in this call we yield CPU
        if (fileContextProvider.didFullLoop()) {
          exit = isTimeout(startTime, waitMillis);
          if (!exit && LOG.isTraceEnabled()) {
            LOG.trace("next(): looped through all directories, yielding CPU");
          }
          exit = exit || !ThreadUtil.sleep(Math.min(getRemainingWaitTime(startTime, waitMillis), MAX_YIELD_TIME));
          fileContextProvider.startNewLoop();
        }
      }
    }
    return chunk;
  }

  /**
   * Determines the offset lag for each active file being read.
   *
   * @param offsetMap the current Offset for file keys.
   * @return map of fileKey to offset.
   */
  public Map<String, Long> getOffsetsLag(Map<String, String> offsetMap) throws IOException{
    return fileContextProvider.getOffsetsLag(offsetMap);
  }

  /**
   * Determines the number of files yet to be processed.
   *
   * @return map of file key (One per directory where files are located) to the number of files
   */
  public Map<String, Long> getPendingFiles() throws IOException{
    return fileContextProvider.getPendingFiles();
  }

  /**
   * Closes all open readers.
   */
  @Override
  public void close() throws IOException {
    if (open) {
      open = false;
      fileContextProvider.close();
    }
  }
}
