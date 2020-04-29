/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.lib.jdbc.multithread.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OffsetQueryUtilTest {

  public void runTestOffsetEscaping(String input) {
    assertEquals(input, OffsetQueryUtil.unescapeOffsetValue(OffsetQueryUtil.escapeOffsetValue(input)));
  }

  @Test
  public void testOffsetEscaping() {
    runTestOffsetEscaping("Hello this is random string");
    runTestOffsetEscaping("Problematical :: String");
    runTestOffsetEscaping("Escape __ String");
    runTestOffsetEscaping("::__:::___::::____");
  }
}
