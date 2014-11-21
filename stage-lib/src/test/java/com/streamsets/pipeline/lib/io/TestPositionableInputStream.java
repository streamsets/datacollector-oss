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
package com.streamsets.pipeline.lib.io;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class TestPositionableInputStream {

  private static final byte[] DATA = {0, 1, 2, 3, 4};

  @Test
  public void testPosition() throws IOException {
    InputStream is = new PositionableInputStream(new ByteArrayInputStream(DATA), 2);
    Assert.assertEquals(2, is.read());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPositionNegative() throws IOException {
    new PositionableInputStream(new ByteArrayInputStream(DATA), -1);
  }

  @Test(expected = IOException.class)
  public void testPositionAfterEOF() throws IOException {
    new PositionableInputStream(new ByteArrayInputStream(DATA), 6);
  }

  @Test
  public void testPositionAtEOF() throws IOException {
    InputStream is = new PositionableInputStream(new ByteArrayInputStream(DATA), 5);
    Assert.assertEquals(-1, is.read());
  }
}
