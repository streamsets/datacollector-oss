/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * A <code>LiveFileChunk</code> is a data segmented of a {@link LiveFile} that is guaranteed to be comprised of
 * full text lines.
 */
public class LiveFileChunk {
  private final LiveFile file;
  private final byte[] data;
  private final Charset charset;
  private final long initialOffset;
  private final int length;
  private final boolean truncated;

  LiveFileChunk(LiveFile file, byte[] data, Charset charset, long initialOffset, int length, boolean truncated) {
    this.file = file;
    this.data = data;
    this.charset = charset;
    this.initialOffset = initialOffset;
    this.length = length;
    this.truncated = truncated;
  }

  /**
   * Returns the file the chunk was read from.
   *
   * @return the file the chunk was read from.
   */
  public LiveFile getFile() {
    return file;
  }

  /**
   * Returns the chunk charset.
   *
   * @return  the chunk charset.
   */
  public Charset getCharset() {
    return charset;
  }

  /**
   * Returns the chunk buffer. It is reference, do not modify.
   *
   * @return the chunk buffer. It is reference, do not modify.
   */
  public byte[] getBuffer() {
    return data;
  }

  /**
   * Returns a {@link Reader} to the data in the chunk.
   * <p/>
   * The {@link Reader} is created using the {@link java.nio.charset.Charset} specified in the {@link LiveFileReader}.
   *
   * @return a {@link Reader} to the data in the chunk.
   */
  public Reader getReader() {
    return new InputStreamReader(new ByteArrayInputStream(data, 0, length), charset);
  }

  /**
   * Returns the byte offset of the chunk in the {@link LiveFile}.
   *
   * @return the byte offset of the chunk in the {@link LiveFile}.
   */
  public long getOffset() {
    return initialOffset;
  }

  /**
   * Returns the byte length of the data in the chunk.
   *
   * @return the byte length of the data in the chunk.
   */
  public int getLength() {
    return length;
  }

  /**
   * Returns if the chunk has been truncated. This happens if the last line of the data chunk exceeds the maximum
   * length specified in the {@link LiveFileReader}.
   *
   * @return <code>true</code> if the chunk has been truncated, <code>false</code> if not.
   */
  public boolean isTruncated() {
    return truncated;
  }

  /**
   * Returns a list with the {@link FileLine} in the chunk. Using <code>FileLine</code>s gives access to the
   * byte offset of each line (which is important when using multi-byte character encodings).
   *
   * @return a list with the {@link FileLine} in the chunk.
   */
  public List<FileLine> getLines() {
    List<FileLine> lines = new ArrayList<>();
    int start = 0;
    for (int i = 0; i < length; i++) {
      if (data[i] == '\n') {
        lines.add(new FileLine(this, start, i + 1 - start));
        start = i + 1;
      } else if (data[i] == '\r') {
        if (i + 1 < length && data[i + 1] == '\n') {
          lines.add(new FileLine(this, start, i + 2 - start));
          start = i + 2;
          i++;
        }
      }
    }
    if (start < length) {
      lines.add(new FileLine(this, start, length - start));
    }
    return lines;
  }

}
