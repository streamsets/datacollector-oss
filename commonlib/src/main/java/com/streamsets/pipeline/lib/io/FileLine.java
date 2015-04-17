/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

/**
 * A <code>FileLine</code> contains the text of a line and its byte offset in a file.
 */
public class FileLine {
  private final LiveFileChunk chunk;
  private final int chunkOffset;
  private final int length;

  public FileLine(LiveFileChunk chunk, int chunkOffset, int length) {
    this.chunk = chunk;
    this.chunkOffset = chunkOffset;
    this.length = length;
  }

  /**
   * Returns the text of the line.
   *
   * @return the text of the line.
   */
  public String getText() {
    return new String(chunk.getBuffer(), chunkOffset, length, chunk.getCharset());
  }

  /**
   * Returns the byte offset of the line in the file.
   *
   * @return the byte offset of the line in the file.
   */
  public long getFileOffset() {
    return chunk.getOffset() + chunkOffset;
  }

  /**
   * Returns the chunk buffer. It is reference, do not modify.
   *
   * @return the chunk buffer. It is reference, do not modify.
   */
  public byte[] getChunkBuffer() {
    return chunk.getBuffer();
  }

  /**
   * Returns the offset of the line within the chunk buffer.
   *
   * @return the offset of the line within the chunk buffer.
   */
  public int getOffset() {
    return chunkOffset;
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
