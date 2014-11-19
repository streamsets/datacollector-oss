/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
