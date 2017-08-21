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

import com.google.common.base.Strings;
import com.streamsets.pipeline.api.ext.io.OverrunException;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;

public class TestOverrunInputStream {

  @Test
  public void testOverrunUnderLimit() throws Exception {
    ByteArrayInputStream is = new ByteArrayInputStream(Strings.repeat("a", 128).getBytes());
    OverrunInputStream ois = new OverrunInputStream(is, 64, true);
    byte[] buff = new byte[128];
    ois.read(buff, 0, 64);
    ois.resetCount();
    ois.read(buff, 0, 64);
  }

  @Test(expected = OverrunException.class)
  public void testOverrunOverLimit() throws Exception {
    ByteArrayInputStream is = new ByteArrayInputStream(Strings.repeat("a", 128).getBytes());
    OverrunInputStream ois = new OverrunInputStream(is, 64, true);
    byte[] buff = new byte[128];
    ois.read(buff, 0, 65);
  }

  @Test
  public void testOverrunOverLimitNotEnabled() throws Exception {
    ByteArrayInputStream is = new ByteArrayInputStream(Strings.repeat("a", 128).getBytes());
    OverrunInputStream ois = new OverrunInputStream(is, 64, false);
    byte[] buff = new byte[128];
    ois.read(buff, 0, 65);
  }

  @Test(expected =  OverrunException.class)
  public void testOverrunOverLimitPostConstructorEnabled() throws Exception {
    ByteArrayInputStream is = new ByteArrayInputStream(Strings.repeat("a", 1280).getBytes());
    OverrunInputStream ois = new OverrunInputStream(is, 64, false);
    byte[] buff = new byte[128];
    try {
      ois.read(buff, 0, 65);
    } catch (OverrunException ex) {
      Assert.fail();
    }
    ois.setEnabled(true);
    buff = new byte[128];
    ois.read(buff, 0, 65);
  }

}
