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

import com.google.common.base.Strings;
import org.junit.Test;

import java.io.Reader;
import java.io.StringReader;

public class TestOverrunReader {

  @Test
  public void testOverrunUnderLimit() throws Exception {
    Reader is = new StringReader(Strings.repeat("a", 128));
    OverrunReader ois = new OverrunReader(is, 64);
    char[] buff = new char[128];
    ois.read(buff, 0, 64);
    ois.resetCharCount();
    ois.read(buff, 0, 64);
  }

  @Test(expected = OverrunException.class)
  public void testOverrunOverLimit() throws Exception {
    Reader is = new StringReader(Strings.repeat("a", 128));
    OverrunReader ois = new OverrunReader(is, 64);
    char[] buff = new char[128];
    ois.read(buff, 0, 65);
  }

}
