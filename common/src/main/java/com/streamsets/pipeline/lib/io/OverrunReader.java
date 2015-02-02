/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import com.streamsets.pipeline.api.impl.Utils;
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
  private boolean enabled;

  public OverrunReader(Reader in, int readLimit, boolean enabled) {
    super(in);
    this.readLimit = readLimit;
    setEnabled(enabled);
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
    if (enabled) {
      resetCount();
    }
  }

  public boolean isEnabled() {
    return enabled;
  }

  @Override
  protected synchronized void afterRead(int n) {
    super.afterRead(n);
    if (isEnabled() && getCount() > readLimit) {
      ExceptionUtils.throwUndeclared(new OverrunException(Utils.format(
          "Reader exceeded the read limit '{}'", readLimit), getPos()));
    }
  }


}
