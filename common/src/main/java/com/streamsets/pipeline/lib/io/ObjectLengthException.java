/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import java.io.IOException;

public class ObjectLengthException extends IOException {
  private long readerOffset;

  public ObjectLengthException(String message, long offset) {
    super(message);
    readerOffset = offset;
  }

  public long getOffset() {
    return readerOffset;
  }

}
