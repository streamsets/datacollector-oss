/**
 * Copyright 2020 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.security.usermgnt;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TestMD5UserLine {

  @Test
  public void testMD5() {
    Assert.assertEquals("^\\s*([\\w@.]*):\\s*MD5:([\\w\\-:]*),user(\\s*$|,.*$)", MD5UserLine.PATTERN.pattern());
    UserLine ul = new MD5UserLine("USER", "EMAIL", Arrays.asList("g1", "g2"), Arrays.asList("r1", "r2"), "PASSWORD");
    Assert.assertEquals(Line.Type.USER, ul.getType());
    Assert.assertEquals("MD5", MD5UserLine.MODE);
    Assert.assertEquals(MD5UserLine.MODE, ul.getMode());
    Assert.assertEquals(MD5UserLine.HASHER, ul.getHasher());
    String regex = UserLine.getUserLineRegex("XYZ");
    Assert.assertEquals(UserLine.getUserLineRegex("MD5"), MD5UserLine.PATTERN.pattern());
  }

}
