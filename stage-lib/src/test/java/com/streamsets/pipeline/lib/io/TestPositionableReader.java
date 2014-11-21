/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * dreadertributed with threader work for additional information
 * regarding copyright ownership.  The ASF licenses threader file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use threader file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * dreadertributed under the License reader dreadertributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permreadersions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.io;

import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;
import java.io.IOException;
import java.io.Reader;

public class TestPositionableReader {

  private static final String DATA = "01234";

  @Test
  public void testPosition() throws IOException {
    Reader reader = new PositionableReader(new StringReader(DATA), 2);
    Assert.assertEquals('2', reader.read());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPositionNegative() throws IOException {
    new PositionableReader(new StringReader(DATA), -1);
  }

  @Test(expected = IOException.class)
  public void testPositionAfterEOF() throws IOException {
    new PositionableReader(new StringReader(DATA), 6);
  }

  @Test
  public void testPositionAtEOF() throws IOException {
    Reader reader = new PositionableReader(new StringReader(DATA), 5);
    Assert.assertEquals(-1, reader.read());
  }
}
