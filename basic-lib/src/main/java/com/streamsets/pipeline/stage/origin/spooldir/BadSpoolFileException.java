/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

public class BadSpoolFileException extends Exception {
  private final String file;
  private final String pos;

  public BadSpoolFileException(String file, String pos, Exception ex) {
    super(ex);
    this.file = file;
    this.pos = pos;
  }

  public String getFile() {
    return file;
  }

  public String getPos() {
    return pos;
  }
}
