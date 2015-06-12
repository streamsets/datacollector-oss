/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import java.nio.charset.Charset;

/**
 * A <code>FileLine</code> contains the text of a line and its byte offset in a file.
 */
public class FileLine {
  private final byte[] buffer;
  private final long fileOffset;
  private final Charset charset;
  private final int offsetInChunk;
  private final int length;
  private String line;

  // creates a FileLine from a raw byte[] buffer
  FileLine(Charset charset, long offsetOfBuffer, byte[] buffer, int offsetInBuffer, int length) {
    this.charset = charset;
    this.fileOffset = offsetOfBuffer +  offsetInBuffer;
    this.buffer = buffer;
    this.offsetInChunk = offsetInBuffer;
    this.length = length;
  }

  // creates a FileLine from the buffer of a chunk, it references the original buffer, no bytes copying
  FileLine(LiveFileChunk chunk, int offsetInChunk, int length) {
    this(chunk.getCharset(), chunk.getOffset(), chunk.getBuffer(), offsetInChunk, length);
  }

  /**
   * Returns the text of the line.
   *
   * @return the text of the line.
   */
  public String getText() {
    if (line == null) {
      line = new String(buffer, offsetInChunk, length, charset);
    }
    return line;
  }

  /**
   * Returns the charset of the buffer.
   *
   * @return the charset of the buffer.
   */
  public Charset getCharset() {
    return charset;
  }

  /**
   * Returns the byte offset of the line in the file.
   *
   * @return the byte offset of the line in the file.
   */
  public long getFileOffset() {
    return fileOffset;
  }

  /**
   * Returns the buffer. It is reference, do not modify.
   *
   * @return the buffer. It is reference, do not modify.
   */
  public byte[] getBuffer() {
    return buffer;
  }

  /**
   * Returns the offset of the line within the buffer.
   *
   * @return the offset of the line within the buffer.
   */
  public int getOffset() {
    return offsetInChunk;
  }

  /**
   * Returns the line length.
   *
   * @return the line length.
   */
  public int getLength() {
    return length;
  }

}
