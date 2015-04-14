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
  private final String text;
  private final long fileOffset;

  FileLine(String text, long fileOffset) {
    this.text = text;
    this.fileOffset = fileOffset;
  }

  /**
   * Returns the text of the line.
   *
   * @return the text of the line.
   */
  public String getText() {
    return text;
  }

  /**
   * Returns the byte offset of the line in the file.
   *
   * @return the byte offset of the line in the file.
   */
  public long getFileOffset() {
    return fileOffset;
  }

}
