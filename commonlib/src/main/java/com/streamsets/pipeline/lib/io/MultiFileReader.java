/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The <code>MultiFileReader</code> is a Reader that allows to read multiple files in a 'tail -f' mode while
 * keeping track of the current offsets and detecting if the files has been renamed.
 * <p/>
 * It builds on top of the {@link LiveFileReader} adding support for reading data from multiple files in different
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

  private final List<FileContext> fileContexts;
  private final Set<String> fileKeys;
  private int startingIdx;
  private final List<FileEvent> events;
  private boolean open;

  /**
   * Creates a <code>MultiFileReader</code> that will scan/read multiple directories for data.
   *
   * @param fileInfos a list with the information for for each directory to scan/read.
   * @param charset the data charset (for all files)
   * @param maxLineLength the maximum line length (for all files)
   * @throws IOException thrown if there was an IO error while creating the reader.
   */
  public MultiFileReader(List<MultiFileInfo> fileInfos, Charset charset, int maxLineLength,
      PostProcessingOptions postProcessing, String archiveDir) throws IOException {
    Utils.checkNotNull(fileInfos, "fileInfos");
    Utils.checkArgument(!fileInfos.isEmpty(), "fileInfos cannot be empty");
    Utils.checkNotNull(charset, "charset");
    Utils.checkArgument(maxLineLength > 1, "maxLineLength must be greater than one");
    Utils.checkNotNull(postProcessing, "postProcessing");
    Utils.checkArgument(
        postProcessing != PostProcessingOptions.ARCHIVE || (archiveDir != null && !archiveDir.isEmpty()),
        "archiveDir cannot be empty if postProcessing is ARCHIVE");
    fileContexts = new ArrayList<>();
    fileKeys = new LinkedHashSet<>();

    archiveDir = (postProcessing == PostProcessingOptions.ARCHIVE) ? archiveDir : null;

    FileEventPublisher eventPublisher = new FileEventPublisher() {
      @Override
      public void publish(FileEvent event) {
        events.add(event);
      }
    };

    for (MultiFileInfo dirInfo : fileInfos) {
      fileContexts.add(new FileContext(dirInfo, charset, maxLineLength, postProcessing, archiveDir, eventPublisher));
      if (fileKeys.contains(dirInfo.getFileKey())) {
        throw new IOException(Utils.format("File '{}' already specified, it cannot be added more than once",
            dirInfo.getFileKey()));
      }
      fileKeys.add(dirInfo.getFileKey());
    }


    events = new ArrayList<>(fileInfos.size() * 2);
    open = true;
    LOG.debug("Opening files: {}", fileKeys);
  }

  /**
   * Sets the directory offsets to use for the next read. To work correctly, the last return offsets should be used or
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
        LOG.trace("Setting offset: directory '{}', file '{}', offset '{}'", fileContext.getMultiFileInfo()
                                                                                      .getFileFullPath(), file,
                  fileOffset);
      }
    }

    // we reset the events on every setOffsets().
    events.clear();
  }

  /**
   * Returns the current directory offsets. The returned offsets should be set before the next read.
   *
   * @return the current directory offsets.
   * @throws IOException thrown if there was an IO error while preparing file offsets.
   */
  public Map<String, String> getOffsets() throws IOException {
    Utils.checkState(open, "Not open");
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
    return map;
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
   * @return the next chunk, or <code>null</code> if there is no next chunk adn teh waiting time passed.
   * @throws IOException thrown if there was an IO error while reading a chunk.
   */
  public LiveFileChunk next(long waitMillis) throws IOException {
    Utils.checkState(open, "Not open");
    waitMillis = (waitMillis > 0) ? waitMillis : 0;
    long startTime = System.currentTimeMillis();
    LiveFileChunk chunk = null;
    boolean exit = false;
    int emptyReadAttempts = 0;
    while (!exit) {
      FileContext fileContext = fileContexts.get(startingIdx);
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
        fileContext.releaseReader();
      } else {
        if (LOG.isTraceEnabled()) {
          LOG.trace("next(): directory '{}', no reader available", fileContext.getMultiFileInfo().getFileFullPath());
        }
      }

      // update startingIdx for future next() call (effective if we exit the while loop in this iteration)
      startingIdx = (startingIdx + 1) % fileContexts.size();

      // check exit conditions (we have a chunk, or we timed-out waitMillis)
      exit = chunk != null;
      if (!exit) {
        emptyReadAttempts++;
        // if we looped thru all dir contexts in this call we yield CPU
        if (emptyReadAttempts == fileContexts.size()) {
          exit = isTimeout(startTime, waitMillis);
          if (!exit && LOG.isTraceEnabled()) {
            LOG.trace("next(): looped through all directories, yielding CPU");
          }
          exit = exit || !ThreadUtil.sleep(Math.min(getRemainingWaitTime(startTime, waitMillis), MAX_YIELD_TIME));
          emptyReadAttempts = 0;
        }
      }
    }
    return chunk;
  }

  /**
   * Closes all open readers.
   */
  public void close() {
    if (open) {
      open = false;
      LOG.debug("Closing files: {}", fileKeys);
      for (FileContext fileContext : fileContexts) {
        try {
          if (fileContext.hasReader()) {
            fileContext.getReader().close();
          }
        } catch (IOException ex) {
          //TODO LOG
        }
      }
    }
  }
}
