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
package com.streamsets.pipeline.api;

import com.streamsets.pipeline.api.impl._ApiUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;

public class Test_DateTypeSupport {

  @Test
  public void testConvertValid() throws Exception {
    _DateTypeSupport support = new _DateTypeSupport();
    Date d = new Date();
    Assert.assertEquals(d, support.convert(d));
    d = _ApiUtils.parse("2014-10-22T13:30Z");
    Assert.assertEquals(d, support.convert("2014-10-22T13:30Z"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertInValid1() {
    new _DateTypeSupport().convert(new Exception());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertInValid2() {
    new _DateTypeSupport().convert("2014");
  }

  @Test
  public void testSnapshot() {
    _DateTypeSupport ts = new _DateTypeSupport();
    Date d = new Date();
    Assert.assertEquals(d, ts.snapshot(d));
    Assert.assertNotSame(d, ts.snapshot(d));
  }

}
