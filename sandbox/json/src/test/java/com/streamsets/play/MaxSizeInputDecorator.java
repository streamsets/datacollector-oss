/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.play;

import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.io.InputDecorator;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

public class MaxSizeInputDecorator extends InputDecorator {
  private int maxSize;
  private OverrunInputStream sis;

  public MaxSizeInputDecorator(int maxSize) {
    this.maxSize = maxSize;
  }

  @Override
  public InputStream decorate(IOContext ctxt, InputStream in) throws IOException {
    if (sis != null) {
      throw new IllegalStateException();
    }
    sis = new OverrunInputStream(in, maxSize);
    return sis;
  }

  @Override
  public InputStream decorate(IOContext ctxt, byte[] src, int offset, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Reader decorate(IOContext ctxt, Reader r) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void resetCount() {
    sis.resetCount();
  }

}
