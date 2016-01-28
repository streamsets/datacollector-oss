/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

  @Override
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
