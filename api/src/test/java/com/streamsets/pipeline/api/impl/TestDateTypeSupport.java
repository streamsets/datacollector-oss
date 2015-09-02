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
package com.streamsets.pipeline.api.impl;

import org.junit.Assert;
import org.junit.Test;

import java.util.Date;

public class TestDateTypeSupport {

  @Test
  public void testCreate() {
    DateTypeSupport ts = new DateTypeSupport();
    Date o = new Date();
    Assert.assertEquals(o, ts.create(o));
    Assert.assertNotSame(o, ts.create(o));
  }

  @Test
  public void testGet() {
    DateTypeSupport ts = new DateTypeSupport();
    Date o = new Date();
    Assert.assertEquals(o, ts.get(o));
    Assert.assertNotSame(o, ts.get(o));
  }

  @Test
  public void testClone() {
    DateTypeSupport ts = new DateTypeSupport();
    Date o = new Date();
    Assert.assertEquals(o, ts.clone(o));
    Assert.assertNotSame(o, ts.clone(o));
  }

  @Test
  public void testConvertValid() throws Exception {
    DateTypeSupport support = new DateTypeSupport();
    Date d = new Date();
    Assert.assertEquals(d, support.convert(d));
    d = Utils.parse("2014-10-22T13:30Z");
    Assert.assertEquals(d, support.convert("2014-10-22T13:30Z"));
    Assert.assertEquals(d, support.convert(d.getTime()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertInValid1() {
    new DateTypeSupport().convert(new Exception());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertInValid2() {
    new DateTypeSupport().convert("2014");
  }

}
