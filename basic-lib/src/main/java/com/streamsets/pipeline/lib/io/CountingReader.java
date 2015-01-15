/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import org.apache.commons.io.input.ProxyReader;

import java.io.IOException;
import java.io.Reader;

public class CountingReader extends ProxyReader  {
  private long pos;
  private long count;
  private long markLimit = 0;
  private long readAfterMark = 0;

  public CountingReader(Reader reader) {
    super(reader);
  }

  @Override
  public long skip(long ln) throws IOException {
    final long skip = super.skip(ln);
    this.count += skip;
    this.pos += skip;
    updateMarkState(skip);
    return skip;
  }

  protected void afterRead(int n) {
    if (n != -1) {
      this.count += n;
      this.pos += n;
      updateMarkState(n);
    }
  }

  private void updateMarkState(long n) {
    if (markLimit > 0) {
      readAfterMark += n;
      if (readAfterMark > markLimit) {
        markLimit = 0;
        readAfterMark = 0;
      }
    }
  }

  public long getPos() {
    return pos;
  }

  public long getCount() {
    return this.count;
  }

  public long resetCount() {
    long tmp = this.count;
    this.count = 0;
    markLimit = 0;
    readAfterMark = 0;
    return tmp;
  }

  @Override
  public void mark(int idx) throws IOException {
    super.mark(idx);
    markLimit = idx;
    readAfterMark = 0;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    count -= readAfterMark;
    pos -= readAfterMark;
  }

}
