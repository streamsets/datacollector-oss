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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

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

  FileLine(long offsetOfBuffer, String str) {
    buffer = null;
    this.charset = StandardCharsets.UTF_8;
    this.fileOffset = offsetOfBuffer;
    offsetInChunk = 0;
    line = str;
    length = str.length();
  }

  // creates a FileLine from the buffer of a chunk, it references the original buffer, no bytes copying
  FileLine(LiveFileChunk chunk, int offsetInChunk, int length) {
    charset = chunk.getCharset();
    fileOffset = chunk.getOffset() +  offsetInChunk;
    buffer = chunk.getBuffer();
    this.offsetInChunk = offsetInChunk;
    this.length = length;
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

  public String toString() {
    return Utils.format("FileLine='{}'", getText());
  }
}
