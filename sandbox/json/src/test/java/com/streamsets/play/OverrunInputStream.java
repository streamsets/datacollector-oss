/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.play;

import org.apache.commons.io.input.CountingInputStream;

import java.io.InputStream;

public class OverrunInputStream extends CountingInputStream {
  private final int maxSegment;

  public OverrunInputStream(InputStream in, int maxSegment) {
    super(in);
    this.maxSegment = maxSegment;
  }

  @Override
  protected synchronized void afterRead(int n) {
    super.afterRead(n);
    if (getCount() > maxSegment) {
      throw new IllegalStateException("COUNT: " + getCount());
    }
  }

}
