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

import java.io.IOException;
import java.io.StringReader;

public class TestCountingReader {

  private static final String DATA = "0123456789";

  @Test
  public void test() throws IOException {
    CountingReader reader = new CountingReader(new StringReader(DATA));
    Assert.assertEquals(0, reader.getCount());
    Assert.assertNotEquals(-1, reader.read());
    Assert.assertEquals(1, reader.getCount());
    Assert.assertEquals(2, reader.read(new char[2]));
    Assert.assertEquals(3, reader.getCount());
    Assert.assertEquals(1, reader.skip(1));
    Assert.assertEquals(4, reader.getCount());
    reader.mark(2);
    Assert.assertEquals(4, reader.getCount());
    Assert.assertNotEquals(-1, reader.read());
    Assert.assertEquals(5, reader.getCount());
    reader.reset();
    Assert.assertEquals(4, reader.getCount());
    reader.mark(1);
    Assert.assertEquals(2, reader.read(new char[2]));
    Assert.assertEquals(6, reader.getCount());
    reader.reset();
    Assert.assertEquals(6, reader.getCount());
    reader.mark(1);
    reader.resetCount();
    Assert.assertEquals(0, reader.getCount());
    reader.reset();
    Assert.assertEquals(0, reader.getCount());
    Assert.assertNotEquals(-1, reader.read());
    Assert.assertEquals(1, reader.getCount());
    reader.mark(1);
    Assert.assertNotEquals(-1, reader.read());
    reader.resetCount();
    Assert.assertEquals(0, reader.getCount());
    reader.reset();
    Assert.assertEquals(0, reader.getCount());
  }

}
