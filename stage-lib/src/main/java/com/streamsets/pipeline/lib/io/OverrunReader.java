/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.lib.util.ExceptionUtils;

import java.io.Reader;

/**
 * Caps amount of data read to avoid OOM issues, max size should be 64K or more ot avoid issues with implicit
 * stream buffers by JDK and libraries.
 */
public class OverrunReader extends CountingReader {

  public static final String READ_LIMIT_SYS_PROP = "overrun.reader.read.limit";

  public static int getDefaultReadLimit() {
    return Integer.parseInt(System.getProperty(READ_LIMIT_SYS_PROP, "102400"));
  }

  private final int readLimit;

  public OverrunReader(Reader in, int readLimit) {
    super(in);
    this.readLimit = readLimit;
  }

  @Override
  protected synchronized void afterRead(int n) {
    super.afterRead(n);
    if (getCount() > readLimit) {
      ExceptionUtils.throwUndeclared(new OverrunException(Utils.format(
          "Reader exceeded the read limit '{}'", readLimit), getPos()));
    }
  }


}
