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

public class TestCommentLine {

  @Test
  public void testComment() {
    Assert.assertEquals("^\\b*#.*$", CommentLine.PATTERN.pattern());
    Line ul = new CommentLine("#foo");
    Assert.assertEquals(Line.Type.COMMENT, ul.getType());
    Assert.assertNull(ul.getId());
    Assert.assertEquals("#foo", ul.getValue());
  }

}
