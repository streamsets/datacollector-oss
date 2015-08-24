/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import java.io.IOException;

public class OverrunException extends IOException {
  private long offset;

  public OverrunException(String message, long offset) {
    this(message, offset, null);
  }

  public OverrunException(String message, long offset, Throwable parent) {
    super(message, parent);
    this.offset = offset;
  }

  public long getStreamOffset() {
    return offset;
  }

}
