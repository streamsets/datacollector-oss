/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.ExceptionUtils;

import java.io.InputStream;

public class OverrunInputStream extends CountingInputStream {

  private final int readLimit;
  private boolean enabled;

  public OverrunInputStream(InputStream in, int readLimit, boolean enabled) {
    super(in);
    this.readLimit = readLimit;
    this.enabled = enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled && readLimit > 0;
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
