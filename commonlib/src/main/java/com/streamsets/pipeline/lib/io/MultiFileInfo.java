/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import com.streamsets.pipeline.config.FileRollMode;

/**
 * The <code>FileInfo</code> encapsulates all the information regarding a directory to read from.
 */
public class MultiFileInfo {
  private final String tag;
  private final String fileFullPath;
  private final FileRollMode fileRollMode;
  private final String pattern;
  private final String firstFile;

  /**
   * Creates a <code>FileInfo</code>
   * @param tag file tag.
   * @param fileFullPath file full path.
   * @param fileRollMode file roll mode.
   * @param pattern file pattern, if any.
   * @param firstFile first file to read.
   */
  public MultiFileInfo(String tag, String fileFullPath, FileRollMode fileRollMode, String pattern, String firstFile) {
    this.tag = tag;
    this.fileFullPath = fileFullPath;
    this.fileRollMode = fileRollMode;
    this.pattern = pattern;
    this.firstFile = firstFile;
  }

  public String getFileKey() {
    return getFileFullPath() + "||" + getPattern();
  }

  public String getTag() {
    return tag;
  }

  public String getFileFullPath() {
    return fileFullPath;
  }

  public FileRollMode getFileRollMode() {
    return fileRollMode;
  }

  public String getPattern() {
    return pattern;
  }

  public String getFirstFile() {
    return firstFile;
  }
}
