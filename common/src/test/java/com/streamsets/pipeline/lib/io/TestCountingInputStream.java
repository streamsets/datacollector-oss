/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

public class TestCountingInputStream {

  private static final String DATA = "0123456789";

  @Test
  public void test() throws IOException {
    CountingInputStream countingInputStream = new CountingInputStream(new ByteArrayInputStream(DATA.getBytes()));
    Assert.assertEquals(0, countingInputStream.getCount());
    Assert.assertNotEquals(-1, countingInputStream.read());
    Assert.assertEquals(1, countingInputStream.getCount());
    Assert.assertEquals(2, countingInputStream.read(new byte[2]));
    Assert.assertEquals(3, countingInputStream.getCount());
    Assert.assertEquals(1, countingInputStream.skip(1));
    Assert.assertEquals(4, countingInputStream.getCount());
    countingInputStream.mark(2);
    Assert.assertEquals(4, countingInputStream.getCount());
    Assert.assertNotEquals(-1, countingInputStream.read());
    Assert.assertEquals(5, countingInputStream.getCount());
    countingInputStream.reset();
    Assert.assertEquals(4, countingInputStream.getCount());
    countingInputStream.mark(1);
    Assert.assertEquals(2, countingInputStream.read(new byte[2]));
    Assert.assertEquals(6, countingInputStream.getCount());
    countingInputStream.reset();
    Assert.assertEquals(6, countingInputStream.getCount());
    countingInputStream.mark(1);
    countingInputStream.resetCount();
    Assert.assertEquals(0, countingInputStream.getCount());
    countingInputStream.reset();
    Assert.assertEquals(0, countingInputStream.getCount());
    Assert.assertNotEquals(-1, countingInputStream.read());
    Assert.assertEquals(1, countingInputStream.getCount());
    countingInputStream.mark(1);
    Assert.assertNotEquals(-1, countingInputStream.read());
    countingInputStream.resetCount();
    Assert.assertEquals(0, countingInputStream.getCount());
    countingInputStream.reset();
    Assert.assertEquals(0, countingInputStream.getCount());
  }

}
