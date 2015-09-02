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

import java.util.HashMap;
import java.util.Map;

public class TestMapTypeSupport {

  @Test
  public void testCreate() {
    MapTypeSupport ts = new MapTypeSupport();
    Map map = new HashMap();
    Assert.assertEquals(map, ts.create(map));
    Assert.assertNotSame(map, ts.create(map));
  }

  @Test
  public void testGet() {
    MapTypeSupport ts = new MapTypeSupport();
    Map map = new HashMap();
    Assert.assertEquals(map, ts.get(map));
    Assert.assertSame(map, ts.get(map));
  }

  @Test
  public void testClone() {
    MapTypeSupport ts = new MapTypeSupport();
    Map map = new HashMap();
    Assert.assertEquals(map, ts.clone(map));
    Assert.assertNotSame(map, ts.clone(map));
  }

  @Test
  public void testConvertValid() throws Exception {
    MapTypeSupport support = new MapTypeSupport();
    Map m = new HashMap<>();
    Assert.assertEquals(m, support.convert(m));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertInValid() {
    new MapTypeSupport().convert(new Exception());
  }

}
