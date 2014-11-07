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
package com.streamsets.pipeline.record;

import org.junit.Assert;
import org.junit.Test;

public class TestReservedPrefixSimpleMap {

  @Test(expected = NullPointerException.class)
  public void testConstructorInvalid1() {
    new ReservedPrefixSimpleMap<Object>(null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorInvalid2() {
    new ReservedPrefixSimpleMap<Object>("", null);
  }

  @Test(expected = NullPointerException.class)
  public void testConstructorInvalid3() {
    new ReservedPrefixSimpleMap<Object>("x", null);
  }

  @Test
  public void testMethods() {
    SimpleMap<String, Object> sm = new VersionedSimpleMap<String, Object>();
    SimpleMap<String, Object> rpsm = new ReservedPrefixSimpleMap<Object>("x", sm);
    try {
      rpsm.put("x", "A");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      //expected
    }
    Assert.assertFalse(rpsm.hasKey("a"));
    Assert.assertTrue(rpsm.getKeys().isEmpty());
    Assert.assertTrue(sm.getValues().isEmpty());
    rpsm.put("a", "A");
    Assert.assertTrue(rpsm.hasKey("a"));
    Assert.assertEquals(1, sm.getKeys().size());
    Assert.assertEquals(1, sm.getValues().size());
    rpsm.remove("a");
    Assert.assertTrue(rpsm.getKeys().isEmpty());
    Assert.assertTrue(rpsm.getValues().isEmpty());
    try {
      rpsm.hasKey("x");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      //expected
    }
    try {
      rpsm.get("x");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      //expected
    }
    try {
      rpsm.remove("x");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      //expected
    }

    sm.put("xx", "a");
    Assert.assertTrue(rpsm.getKeys().isEmpty());
    Assert.assertTrue(rpsm.getValues().isEmpty());
    try {
      rpsm.hasKey("xx");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      //expected
    }
    try {
      rpsm.get("xx");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      //expected
    }
    try {
      rpsm.remove("xx");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      //expected
    }

  }

}
