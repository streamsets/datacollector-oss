/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.lib.util.ExceptionUtils;
import org.apache.commons.io.input.CountingInputStream;

import java.io.InputStream;

/**
 * Caps amount of data read to avoid OOM issues, max size should be 64K or more ot avoid issues with implicit
 * stream buffers by JDK and libraries.
 *
 * IMPORTANT: commons-io CountingInputStream does not take into account mark()/reset() calls, BUG?
 */
public class OverrunInputStream extends CountingInputStream {
  private final int maxUnsupervisedReadSize;

  public OverrunInputStream(InputStream in, int maxUnsupervisedReadSize) {
    super(in);
    this.maxUnsupervisedReadSize = maxUnsupervisedReadSize;
  }

  @Override
  protected synchronized void afterRead(int n) {
    super.afterRead(n);
    if (getCount() > maxUnsupervisedReadSize) {
      ExceptionUtils.throwUndeclared(new OverrunException(Utils.format(
          "Stream exceeded the maximum unsupervised read size '{}'", maxUnsupervisedReadSize), 0));
    }
  }

}
