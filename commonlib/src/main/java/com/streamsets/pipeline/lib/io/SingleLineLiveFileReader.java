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
import com.streamsets.pipeline.lib.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

/**
 * A <code>SingleLiveFileReader</code> is a Reader that allows to read a file in a 'tail -f' mode while keeping track
 * of the current offset and detecting if the file has been renamed.
 * <p/>
 * It tails one line at the time. To tail log files with multi-line logs (i.e. Log4j logs with stack traces, MySQL logs)
 * use the {@link MultiLineLiveFileReader}.
 * <p/>
 * The <code>LiveFileReader</code> uses the <code>Iterator</code> pattern to get chunks of lines of the file.
 * <p/>
 * The <code>LiveFileReader</code> performs non-blocking read operations (using Java NIO).
 * <p/>
 * IMPORTANT: The offsets are always on bytes, we are working with charsets where CR and LF are always one byte
 * (ie UTF-8 or ASCII)
 * <p/>
 * IMPORTANT: The provided charset must encode LF and CR as '0x0A' and '0x0D' respectively.
 */
public class SingleLineLiveFileReader implements LiveFileReader {
  private static final Logger LOG = LoggerFactory.getLogger(SingleLineLiveFileReader.class);

  // we refresh the LiveFile every 500 msecs, to detect if it has been renamed
  static final long REFRESH_INTERVAL = Integer.parseInt(System.getProperty("LiveFileReader.refresh.ms", "500"));

  // we sleep for 10 millisec to yield CPU
  private static final long YIELD_INTERVAL = Integer.parseInt(System.getProperty("LiveFileReader.yield.ms", "10"));

  private final RollMode rollMode;
  private final LiveFile originalFile;
  private String tag;
  private LiveFile currentFile;
  private final Charset charset;

  private long offset;
  private boolean truncateMode;

  private final SeekableByteChannel channel;

  private final ByteBuffer buffer;
  private final byte[] chunkBytes;

  private boolean open;
  private long lastLiveFileRefresh;

  private int lastPosCheckedForEol;

  private boolean rolled;

  // negative offset means we are in truncate mode, we need to discard data until we find an EOL

  /**
   * Creates a <code>SingleLiveFileReader</code>.
   *
   * @param rollMode {@link RollMode} the file roll mode.
   * @param tag the file tag.
   * @param file {@link LiveFile} of the file to read.
   * @param charset {@link Charset} of the file, the returned {@link LiveFileChunk} will have a {@link Reader}
   *                               using this character set.
   * @param offset offset in bytes to start reading the file from. If the offset is a negative number its absolute value
   *               will be used and from there data will be ignored until the first EOL. This allows handling offsets
   *               of truncated lines.
   * @param maxLineLen the maximum line length including the EOL characters, if the length is exceeded, the line will
   *                   be truncated.
   * @throws IOException thrown if the file could not be opened or the specified offset is beyond the current file
   * length.
   * @throws IllegalArgumentException thrown if the the provided charset must encode LF and CR as '0x0A' and '0x0D'
   * respectively.
   */
  public SingleLineLiveFileReader(RollMode rollMode, String tag, LiveFile file, Charset charset, long offset,
      int maxLineLen)
      throws IOException {
    Utils.checkNotNull(rollMode, "rollMode");
    Utils.checkNotNull(file, "file");
    Utils.checkNotNull(charset, "charset");
    Utils.checkArgument(maxLineLen > 1, "maxLineLen must greater than 1");
    validateCharset(charset, '\n', "\\n");
    validateCharset(charset, '\r', "\\r");
    this.rollMode = rollMode;
    this.tag = tag;
    this.originalFile = file;
    this.charset = charset;

    this.offset = Math.abs(offset);
    truncateMode = offset < 0;

    currentFile = originalFile.refresh();
    if (!currentFile.equals(originalFile)) {
      LOG.debug("Original file '{}' refreshed to '{}'", file, currentFile);
    }

    channel = Files.newByteChannel(currentFile.getPath(), StandardOpenOption.READ);
    open = true;

    long actualSize;
    try {
      actualSize = channel.size();
    } catch (IOException ex) {
      closeChannel();
      throw ex;
    }

    if (offset > actualSize) {
      channel.close();
      throw new IOException(Utils.format("File '{}', offset '{}' beyond file size '{}'", currentFile.getPath(), offset,
                                         actualSize));
    }

    try {
      channel.position(this.offset);
    } catch (IOException ex) {
      closeChannel();
      throw ex;
    }
    LOG.debug("File '{}', positioned at offset '{}'", currentFile, offset);

    buffer = ByteBuffer.allocate(maxLineLen);
    chunkBytes = new byte[maxLineLen];

    lastPosCheckedForEol = 0;
  }

  private void closeChannel() {
    if (open) {
      try {
        open = false;
        channel.close();
      } catch (IOException ex) {
        //NOP
      }
    }
  }

  private void validateCharset(Charset charset, char c, String cStr) {
    ByteBuffer bf = charset.encode("" + c);
    if (bf.limit() != 1) {
      throw new IllegalArgumentException(Utils.format("Charset '{}' does not encode character '{}' in one byte",
                                                      charset, cStr));
    }
    byte b = bf.get();
    if (b != (byte)c) {
      throw new IllegalArgumentException(Utils.format("Charset '{}' does not encode character '{}' as '{}'",
                                                      charset, cStr, c));
    }
  }

  @Override
  public LiveFile getLiveFile() {
    return currentFile;
  }

  @Override
  public Charset getCharset() {
    return charset;
  }

  // offset will be negative if we are in truncate mode.

  @Override
  public long getOffset() {
    Utils.checkState(open, Utils.formatL("LiveFileReder for '{}' is not open", currentFile));
    return (truncateMode) ? -offset : offset;
  }

  @Override
  public boolean hasNext() throws IOException {
    Utils.checkState(open, Utils.formatL("LiveFileReader for '{}' is not open", currentFile));
    // the buffer is dirty, or the file is still live, or the channel pos is less than the file length
    return (buffer.position() > 0) || !isEof();
  }

  @Override
  public LiveFileChunk next(long waitMillis) throws IOException {
    Utils.checkArgument(waitMillis >= 0, "waitMillis must equal or greater than zero");
    Utils.checkState(open, Utils.formatL("LiveFileReader for '{}' is not open", currentFile));
    LiveFileChunk liveFileChunk = null;
    long start = System.currentTimeMillis() + waitMillis;
    try {
      while (true) {
        if (!hasNext()) {
          break;
        }
        if (truncateMode) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("File '{}' at offset '{} in fast forward mode", currentFile, channel.position());
          }
          truncateMode = fastForward();
        }
        if (!truncateMode) {
          liveFileChunk = readChunk();
          if (LOG.isTraceEnabled()) {
            LOG.trace("File '{}' at offset '{} got chunk '{}'", currentFile, channel.position(), liveFileChunk != null);
          }
          if (liveFileChunk != null) {
            break;
          }
        }
        if (System.currentTimeMillis() - start >= 0) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("File '{}' at offset '{} timed out while waiting for chunk", currentFile, channel.position());
          }
          //wait timeout
          break;
        }
        //yielding CPU while in wait loop
        if (!ThreadUtil.sleep(YIELD_INTERVAL)) {
          LOG.trace("File '{}' at offset '{} interrupted while yielding CPU", currentFile, channel.position());
          break;
        }
      }
      offset = channel.position() - buffer.position();
      return liveFileChunk;
    } catch (IOException ex) {
      closeChannel();
      throw ex;
    }
  }

  @Override
  public void close() throws IOException {
    if (open) {
      open = false;
      channel.close();
    }
  }

  // IMPLEMENTATION

  // returns true if still in truncate mode, false otherwise
  private boolean fastForward() throws IOException {
    try {
      boolean stillTruncate;
      buffer.clear();
      if (channel.read(buffer) > -1 || isEof()) {
        //set the buffer into read from mode
        buffer.flip();
        //we have data, lets look for the first EOL in it.
        int firstEolIdx = findEndOfFirstLine(buffer);
        if (firstEolIdx > -1) {
          // set position to position after first EOL
          buffer.position(firstEolIdx + 1);
          // set the buffer back into write into mode keeping data after first EOL
          buffer.compact();
          stillTruncate = false;
          offset = channel.position() - buffer.position();
        } else {
          // no EOL yet
          // whatever was read will be discarded on next next() call
          stillTruncate = true;
          offset = channel.position();
        }
      } else {
        // no data read
        // whatever was read will be discarded on next next() call
        stillTruncate = true;
        offset = channel.position();
      }
      return stillTruncate;
    } catch (IOException ex) {
      closeChannel();
      throw ex;
    }
  }

  private LiveFileChunk readChunk() throws IOException {
    try {
      LiveFileChunk liveFileChunk = null;
      if (channel.read(buffer) > 0 || buffer.limit() - buffer.position() > 0 || isEof()) {
        // we have data, set the buffer into read from mode
        buffer.flip();
        // lets look for the last EOL in it
        int lastEolIdx = (isEof()) ? buffer.limit() : findEndOfLastLine(buffer);
        if (lastEolIdx > -1) {
          // we have an EOL in the buffer or we are at the end of the file
          int chunkSize = lastEolIdx - buffer.position();
          buffer.get(chunkBytes, 0, chunkSize);
          // create reader with exactly the chunk
          liveFileChunk = new LiveFileChunk(tag, currentFile, charset, chunkBytes, offset, chunkSize, false);
        } else if (buffer.limit() == buffer.capacity()) {
          // buffer is full and we don't have an EOL, return truncated chunk and go into truncate mode.
          // we have an EOL in the buffer or we are at the end of the file
          int chunkSize = buffer.limit() - buffer.position();
          buffer.get(chunkBytes, 0, chunkSize);
          // create reader with exactly the chunk
          liveFileChunk = new LiveFileChunk(tag, currentFile, charset, chunkBytes, offset, chunkSize, true);
          truncateMode = true;
        } else {
          // we don't have an EOL and the buffer is not full, no chunk in this read
          liveFileChunk = null;
        }
        // set the buffer back into write into mode with the leftover data
        buffer.compact();
        // correcting next position in buffer scanned for EOL to reflect post compact() position.
        lastPosCheckedForEol = buffer.position();
      }
      return liveFileChunk;
    } catch (IOException ex) {
      closeChannel();
      throw ex;
    }
  }

  private boolean isEof() throws IOException {
    try {
      if (!rolled) {
        if (originalFile.equals(currentFile) && System.currentTimeMillis() - lastLiveFileRefresh > REFRESH_INTERVAL) {
          currentFile = originalFile.refresh();
          if (!currentFile.equals(originalFile)) {
            LOG.debug("Original file '{}' refreshed to '{}'", originalFile, currentFile);
          }
          rolled = rollMode.isFileRolled(currentFile);
          lastLiveFileRefresh = System.currentTimeMillis();
        }
      }
      return rolled && channel.position() >= channel.size();
    } catch (IOException ex) {
      closeChannel();
      throw ex;
    }
  }

  private int findEndOfLastLine(ByteBuffer buffer) {
    for (int i = buffer.limit() - 1; i > lastPosCheckedForEol; i--) {
      // as we are going backwards, this will handle \r\n EOLs as well without producing extra EOLs
      // and if a buffer ends in \r, the last line will be kept as incomplete until the next chunk.
      if (buffer.get(i) == '\n') {
        return i + 1; // including EOL character
      }
    }
    return -1;
  }

  private int findEndOfFirstLine(ByteBuffer buffer) {
    for (int i = buffer.position(); i < buffer.limit(); i++) {
      if (buffer.get(i) == '\n') {
        return i;
      }
      if (buffer.get(i) == '\r') {
        // handling \r\n EOLs, if the buffer ends exactly after \n, then the next buffer read will work because of the
        // \n detection and no extra EOLs will be produced. Also, note this method is used only in truncated mode
        // doing fastforward to the next line.
        if (i + 1 < buffer.limit() && buffer.get(i + 1) == '\n') {
          return i + 1;
        }
        return i;
      }
    }
    return -1;
  }

}
