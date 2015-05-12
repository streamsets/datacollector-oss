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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The <code>MultiDirectoryReader</code> is a Reader that allows to read multiple files in a 'tail -f' mode while
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
public class MultiDirectoryReader implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(MultiDirectoryReader.class);

  private final static long MAX_YIELD_TIME = Integer.parseInt(System.getProperty("MultiDirectoryReader.yield.ms", "500"));

  /**
   * The <code>DirectoryInfo</code> encapsulates all the information regarding a directory to read from.
   */
  public static class DirectoryInfo {
    private final String dirName;
    private final RollMode rollMode;
    private final String fileName;
    private final String firstFile;

    /**
     * Creates a <code>DirectoryInfo</code>
     *
     * @param dirName directory path.
     * @param rollMode file roll mode.
     * @param fileName live file name.
     * @param firstFile first file to read.
     */
    public DirectoryInfo(String dirName, RollMode rollMode, String fileName, String firstFile) {
      this.dirName = dirName;
      this.rollMode = rollMode;
      this.fileName = fileName;
      this.firstFile = firstFile;
    }
  }

  /**
   * The <code>DirectoryContext</code> encapsulates all live information about a directory being scanned/read.
   */
  private class DirectoryContext {
    private final DirectoryInfo dirInfo;
    private final LiveDirectoryScanner scanner;
    private LiveFile currentFile;
    private LiveFileReader reader;
    private LiveFile startingCurrentFileName;
    private long startingOffset;

    public DirectoryContext(DirectoryInfo dirInfo) throws IOException {
      this.dirInfo = dirInfo;
      scanner = new LiveDirectoryScanner(dirInfo.dirName, dirInfo.fileName, dirInfo.firstFile, dirInfo.rollMode);
    }

    // prepares and gets the reader if available before a read.
    private LiveFileReader getReader() throws IOException {
      if (reader == null) {
        currentFile = startingCurrentFileName;
        long fileOffset = startingOffset;

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
          reader = new LiveFileReader(dirInfo.rollMode, currentFile, charset, fileOffset, maxLineLength);
          if (fileOffset == 0) {
            // adding file start event
            events.add(new Event(currentFile, true));
          }
        }
      }
      return reader;
    }

    // updates reader and offsets after a read.
    private void releaseReader() throws IOException {
      // update starting offsets for next invocation either cold (no reader) or hot (reader)
      if (!reader.hasNext()) {
        // reached EOF
        reader.close();
        reader = null;
        //using Long.MAX_VALUE to signal we reach the end of the file and next iteration should get the next file.
        startingCurrentFileName = currentFile;
        startingOffset = Long.MAX_VALUE;

        // adding file end event
        LiveFile file = currentFile.refresh();
        events.add(new Event(file, false));
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
        startingCurrentFileName = currentFile;
        startingOffset = reader.getOffset();
      }
    }
  }

  private final List<DirectoryContext> dirContexts;
  private final Set<String> dirNames;
  private final Charset charset;
  private final int maxLineLength;
  private final PostProcessingOptions postProcessing;
  private final String archiveDir;
  private int startingIdx;
  private final List<Event> events;
  private boolean open;

  /**
   * Creates a <code>MultiDirectoryReader</code> that will scan/read multiple directories for data.
   *
   * @param dirInfos a list with the information for for each directory to scan/read.
   * @param charset the data charset (for all files)
   * @param maxLineLength the maximum line length (for all files)
   * @throws IOException thrown if there was an IO error while creating the reader.
   */
  public MultiDirectoryReader(List<DirectoryInfo> dirInfos, Charset charset, int maxLineLength,
      PostProcessingOptions postProcessing, String archiveDir) throws IOException {
    Utils.checkNotNull(dirInfos, "dirInfos");
    Utils.checkArgument(!dirInfos.isEmpty(), "dirInfos cannot be empty");
    Utils.checkNotNull(charset, "charset");
    Utils.checkArgument(maxLineLength > 1, "maxLineLength must be greater than one");
    Utils.checkNotNull(postProcessing, "postProcessing");
    Utils.checkArgument(postProcessing != PostProcessingOptions.ARCHIVE || (archiveDir != null && !archiveDir.isEmpty()),
                        "archiveDir cannot be empty if postProcessing is ARCHIVE");
    dirContexts = new ArrayList<>();
    dirNames = new LinkedHashSet<>();

    this.charset = charset;
    this.maxLineLength = maxLineLength;
    this.postProcessing = postProcessing;
    this.archiveDir = (postProcessing == PostProcessingOptions.ARCHIVE) ? archiveDir : null;

    for (DirectoryInfo dirInfo : dirInfos) {
      dirContexts.add(new DirectoryContext(dirInfo));
      if (dirNames.contains(dirInfo.dirName)) {
        throw new IOException(Utils.format("Directory '{}' already specified, it cannot be added more than once",
                                           dirInfo.dirName));
      }
      dirNames.add(dirInfo.dirName);
    }


    events = new ArrayList<>(dirInfos.size() * 2);
    open = true;
    LOG.debug("Opening directories: {}", dirNames);
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
    for (DirectoryContext dirContext : dirContexts) {
      String offset = offsets.get(dirContext.dirInfo.dirName);
      LiveFile file = null;
      long fileOffset = 0;
      if (offset != null && !offset.isEmpty()) {
        String[] split = offset.split("::", 2);
        file = LiveFile.deserialize(split[1]).refresh();
        fileOffset = Long.parseLong(split[0]);
      }
      dirContext.startingCurrentFileName = file;
      dirContext.startingOffset = fileOffset;
      if (LOG.isTraceEnabled()) {
        LOG.trace("Setting offset: directory '{}', file '{}', offset '{}'", dirContext.dirInfo.dirName, file,
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
    for (DirectoryContext dirContext : dirContexts) {
      LiveFile file;
      long fileOffset;
      if (dirContext.reader == null) {
        file = dirContext.startingCurrentFileName;
        fileOffset = dirContext.startingOffset;
      } else if (dirContext.reader.hasNext()) {
        file = dirContext.reader.getLiveFile();
        fileOffset = dirContext.reader.getOffset();
      } else {
        file = dirContext.reader.getLiveFile();
        fileOffset = Long.MAX_VALUE;
      }
      String offset = (file == null) ? "" : Long.toString(fileOffset) + "::" + file.serialize();
      map.put(dirContext.dirInfo.dirName, offset);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Reporting offset: directory '{}', file '{}', offset '{}'", dirContext.dirInfo.dirName, file,
                  fileOffset);
      }
    }
    return map;
  }

  /**
   * File events generated by the {@link MultiDirectoryReader} when starting and ending reading a file.
   */
  public static class Event {
    private final LiveFile file;
    private final boolean start;

    Event(LiveFile file, boolean start) {
      this.file = Utils.checkNotNull(file, "file");
      this.start = start;
    }

    /**
     * Indicates if its a file start reading event.
     *
     * @return <code>true</code> if starting the file, <code>false</code> otherwise.
     */
    public boolean isStart() {
      return start;
    }

    /**
     * Indicates if its a file end reading event.
     *
     * @return <code>true</code> if ending the file, <code>false</code> otherwise.
     */
    public boolean isEnd() {
      return !start;
    }

    /**
     * Returns the <code>LiveFile</code> triggering the event.
     *
     * @return the <code>LiveFile</code> triggering the event.
     */
    public LiveFile getFile() {
      return file;
    }

    public String toString() {
      return Utils.format("MultiDirectoryReader.Event[file='{}' action='{}'", file, (start) ? "START" : "END");
    }

    @Override
    public int hashCode() {
      return file.hashCode() + ((start) ? 1 : -1);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (obj instanceof Event) {
        Event other = (Event) obj;
        return file.equals(other.file) && start == other.start;
      } else {
        return false;
      }
    }
  }

  /**
   * Returns all file events (start and end) since the last {@link #setOffsets(java.util.Map)} call.
   *
   * @return all files events.
   */
  public List<Event> getEvents() {
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
      DirectoryContext dirContext = dirContexts.get(startingIdx);
      LiveFileReader reader = dirContext.getReader();
      if (reader != null) {
        if (reader.hasNext()) {
          chunk = reader.next(0);
          if (LOG.isTraceEnabled()) {
            LOG.trace("next(): directory '{}', file '{}', offset '{}' got data '{}'", dirContext.dirInfo.dirName,
                      reader.getLiveFile(), reader.getOffset(), chunk != null);
          }
        } else {
          if (LOG.isTraceEnabled()) {
            LOG.trace("next(): directory '{}', file '{}', offset '{}' EOF reached", dirContext.dirInfo.dirName,
                      reader.getLiveFile(), reader.getOffset());
          }
        }
        dirContext.releaseReader();
      } else {
        if (LOG.isTraceEnabled()) {
          LOG.trace("next(): directory '{}', no reader available", dirContext.dirInfo.dirName);
        }
      }

      // update startingIdx for future next() call (effective if we exit the while loop in this iteration)
      startingIdx = (startingIdx + 1) % dirContexts.size();

      // check exit conditions (we have a chunk, or we timed-out waitMillis)
      exit = chunk != null;
      if (!exit) {
        emptyReadAttempts++;
        // if we looped thru all dir contexts in this call we yield CPU
        if (emptyReadAttempts == dirContexts.size()) {
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
      LOG.debug("Closing directories: {}", dirNames);
      for (DirectoryContext dirContext : dirContexts) {
        try {
          if (dirContext.reader != null) {
            dirContext.reader.close();
          }
        } catch (IOException ex) {
          //TODO LOG
        }
      }
    }
  }

}
