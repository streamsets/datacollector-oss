/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

/**
 * A <code>LiveFileReader</code> is a Reader that allows to read in a file in a 'tail -f' mode while keeping track
 * of the current offset and detecting if the file has been renamed.
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
public class LiveFileReader implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(LiveFileReader.class);

  // we refresh the LiveFile every 500 msecs, to detect if it has been renamed
  static final long REFRESH_INTERVAL = Integer.parseInt(System.getProperty("LiveFileReader.refresh.ms", "500"));

  // we sleep for 10 millisec to yield CPU
  private static final long YIELD_INTERVAL = Integer.parseInt(System.getProperty("LiveFileReader.yield.ms", "10"));

  private final LiveFile originalFile;
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


  // negative offset means we are in truncate mode, we need to discard data until we find an EOL

  /**
   * Creates a <code>LiveFileReader</code>.
   *
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
  public LiveFileReader(LiveFile file, Charset charset, long offset, int maxLineLen) throws IOException {
    Utils.checkNotNull(file, "file");
    Utils.checkNotNull(charset, "charset");
    Utils.checkArgument(maxLineLen > 1, "maxLineLen must greater than 1");
    validateCharset(charset, '\n', "\\n");
    validateCharset(charset, '\r', "\\r");
    this.originalFile = file;
    this.charset = charset;

    this.offset = Math.abs(offset);
    truncateMode = offset < 0;

    currentFile = originalFile.refresh();
    if (!currentFile.equals(originalFile)) {
      LOG.debug("Original file '{}' refreshed to '{}'", file, currentFile);
    }

    channel = Files.newByteChannel(currentFile.getPath(), StandardOpenOption.READ);
    if (offset > channel.size()) {
      throw new IOException(Utils.format("File '{}', offset '{}' beyond file size '{}'", currentFile.getPath(), offset,
                                         channel.size()));
    }
    channel.position(this.offset);
    LOG.debug("File '{}', positioned at offset '{}'", currentFile, offset);

    buffer = ByteBuffer.allocate(maxLineLen);
    chunkBytes = new byte[maxLineLen];

    lastPosCheckedForEol = 0;

    open = true;
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

  /**
   * Returns the {@link LiveFile} of the reader.
   *
   * @return the {@link LiveFile} of the reader.
   */
  public LiveFile getLiveFile() {
    return currentFile;
  }

  /**
   * Returns the charset of the reader.
   *
   * @return  the charset of the reader.
   */
  public Charset getCharset() {
    return charset;
  }

  // offset will be negative if we are in truncate mode.

  /**
   * Returns the reader offset.
   * <p/>
   * NOTE: The reader offset will be a negative number of the reader is in truncate mode (the last line of the
   * last chunk exceeded the maximum length).
   * @return the reader offset.
   */
  public long getOffset() {
    Utils.checkState(open, Utils.formatL("LiveFileReder for '{}' is not open", currentFile));
    return (truncateMode) ? -offset : offset;
  }

  /**
   * Indicates if the reader has more data or the EOF has been reached. Note that if the {@link LiveFile} is the original
   * one and we are at the EOF we are in 'tail -f' mode, only when the file has been renamed we reached EOF.
   *
   * @return <code>true</code> if the reader has more data, <code>false</code> otherwise.
   * @throws IOException thrown if there was an error while determining if there is more data or not.
   */
  public boolean hasNext() throws IOException {
    Utils.checkState(open, Utils.formatL("LiveFileReader for '{}' is not open", currentFile));
    // the buffer is dirty, or the file is still live, or the channel pos is less than the file length
    return (buffer.position() > 0) || !isEof();
  }

  /**
   * Returns the next chunk of data from the reader if available, or <code>null</code> if there is no data available
   * yet.
   * <p/>
   * This method can be called only if {@link #hasNext()} returned <code>true</code>.
   *
   * @param waitMillis milliseconds to block while waiting for more data, use zero for no wait.
   * @return a {@link LiveFileChunk} with a chunk of data, or <code>null</code> if no data is yet available.
   * @throws IOException thrown if there was a problem while reading the chunk of data
   * @throws InterruptedException thrown if thread was interrupted while the reader was waiting for data.
   */
  public LiveFileChunk next(long waitMillis) throws IOException, InterruptedException {
    Utils.checkArgument(waitMillis >= 0, "waitMillis must equal or greater than zero");
    Utils.checkState(open, Utils.formatL("LiveFileReader for '{}' is not open", currentFile));
    Utils.checkState(hasNext(), Utils.formatL("LiveFileReader for '{}' has reached EOL", currentFile));
    LiveFileChunk liveFileChunk = null;
    long start = System.currentTimeMillis() + waitMillis;
    while (true) {
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
      Thread.sleep(YIELD_INTERVAL);
    }
    offset = channel.position() - buffer.position();
    return liveFileChunk;
  }

  /**
   * Closes the reader.
   *
   * @throws IOException thrown if the reader could not be closed properly.
   */
  @Override
  public void close() throws IOException {
    if (open) {
      channel.close();
    }
  }

  // IMPLEMENTATION

  // returns true if still in truncate mode, false otherwise
  private boolean fastForward() throws IOException {
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
  }

  private LiveFileChunk readChunk() throws IOException {
    LiveFileChunk liveFileChunk = null;
    if (channel.read(buffer) > 0 || buffer.limit() - buffer.position() > 0 || isEof()) {
      // set the buffer into read from mode
      buffer.flip();
      if (buffer.position() < buffer.limit()) {
        // we have data, lets look for the last EOL in it
        int lastEolIdx = (isEof()) ? buffer.limit() : findEndOfLastLine(buffer);
        if (lastEolIdx > -1) {
          // we have an EOL in the buffer or we are at the end of the file
          int chunkSize =lastEolIdx - buffer.position();
          buffer.get(chunkBytes, 0, chunkSize);
          // create reader with exactly the chunk
          Reader reader = new InputStreamReader(new ByteArrayInputStream(chunkBytes, 0, chunkSize), charset);
          liveFileChunk = new LiveFileChunk(chunkBytes, charset, offset, chunkSize, false);
        } else if (buffer.limit() == buffer.capacity()) {
          // buffer is full and we don't have an EOL, return truncated chunk and go into truncate mode.
          // we have an EOL in the buffer or we are at the end of the file
          int chunkSize = buffer.limit() - buffer.position();
          buffer.get(chunkBytes, 0, chunkSize);
          // create reader with exactly the chunk
          Reader reader = new InputStreamReader(new ByteArrayInputStream(chunkBytes, 0, chunkSize), charset);
          liveFileChunk = new LiveFileChunk(chunkBytes, charset, offset, chunkSize, true);
          truncateMode = true;
        } else {
          // we don't have an EOL and the buffer is not full, no chunk on this read
          liveFileChunk = null;
        }
      }
      // correcting next position in buffer scanned for EOL to reflect post compact() position.
      lastPosCheckedForEol = buffer.limit() - buffer.position() + 1;
      // set the buffer back into write into mode with the leftover data
      buffer.compact();
    }
    return liveFileChunk;
  }

  private boolean isEof() throws IOException {
    if (originalFile.equals(currentFile) && System.currentTimeMillis() - lastLiveFileRefresh > REFRESH_INTERVAL) {
      currentFile = originalFile.refresh();
      if (!currentFile.equals(originalFile)) {
        LOG.debug("Original file '{}' refreshed to '{}'", originalFile, currentFile);
      }
      lastLiveFileRefresh = System.currentTimeMillis();
    }
    return !originalFile.equals(currentFile) && channel.position() == channel.size();
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
