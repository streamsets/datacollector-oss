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
package com.streamsets.pipeline.api.v2.impl;

import com.streamsets.pipeline.api.v2.SimpleMap;
import org.junit.Assert;
import org.junit.Test;

public class TestInheritMap {

  @Test
  public void testNoParent() {
    SimpleMap<String, String> m = new InheritableSimpleMap<String, String>();
    Assert.assertFalse(m.hasKey("a"));
    Assert.assertTrue(m.getKeys().isEmpty());
    Assert.assertEquals(null, m.get("a"));
    m.put("a", "A");
    Assert.assertEquals("A", m.get("a"));
    Assert.assertTrue(m.hasKey("a"));
    Assert.assertTrue(m.getKeys().contains("a"));
    Assert.assertEquals(1, m.getKeys().size());
    m.remove("a");
    Assert.assertFalse(m.hasKey("a"));
    Assert.assertTrue(m.getKeys().isEmpty());
    Assert.assertEquals(null, m.get("a"));
  }

  @Test
  public void testEmptyParent() {
    SimpleMap<String, String> m = new InheritableSimpleMap<String, String>();
    m = new InheritableSimpleMap<String, String>(m);
    Assert.assertFalse(m.hasKey("a"));
    Assert.assertTrue(m.getKeys().isEmpty());
    Assert.assertEquals(null, m.get("a"));
    m.put("a", "A");
    Assert.assertEquals("A", m.get("a"));
    Assert.assertTrue(m.hasKey("a"));
    Assert.assertTrue(m.getKeys().contains("a"));
    Assert.assertEquals(1, m.getKeys().size());
    m.remove("a");
    Assert.assertFalse(m.hasKey("a"));
    Assert.assertTrue(m.getKeys().isEmpty());
    Assert.assertEquals(null, m.get("a"));
  }

  @Test
  public void testParentWithEmptyChild() {
    SimpleMap<String, String> m = new InheritableSimpleMap<String, String>();
    m.put("a", "A");
    m = new InheritableSimpleMap<String, String>(m);
    Assert.assertEquals("A", m.get("a"));
    Assert.assertTrue(m.hasKey("a"));
    Assert.assertTrue(m.getKeys().contains("a"));
    Assert.assertEquals(1, m.getKeys().size());
  }

  @Test
  public void testParentWithChildNoShadowing() {
    SimpleMap<String, String> m = new InheritableSimpleMap<String, String>();
    m.put("b", "B");
    m = new InheritableSimpleMap<String, String>(m);
    Assert.assertTrue(m.hasKey("b"));
    Assert.assertEquals(1, m.getKeys().size());
    Assert.assertTrue(m.getKeys().contains("b"));
    m.put("a", "A");
    Assert.assertEquals("A", m.get("a"));
    Assert.assertTrue(m.hasKey("a"));
    Assert.assertEquals(2, m.getKeys().size());
    Assert.assertTrue(m.getKeys().contains("b"));
    Assert.assertTrue(m.getKeys().contains("a"));
    m.remove("a");
    Assert.assertFalse(m.hasKey("a"));
    Assert.assertTrue(m.hasKey("b"));
    Assert.assertEquals(1, m.getKeys().size());
    Assert.assertTrue(m.getKeys().contains("b"));
    Assert.assertEquals(null, m.get("a"));
  }

  @Test
  public void testParentWithChildShadowing() {
    SimpleMap<String, String> m = new InheritableSimpleMap<String, String>();
    m.put("a", "A");
    m = new InheritableSimpleMap<String, String>(m);
    Assert.assertEquals("A", m.get("a"));
    m.put("a", "B");
    Assert.assertEquals("B", m.get("a"));
    Assert.assertTrue(m.hasKey("a"));
    Assert.assertEquals(1, m.getKeys().size());
    Assert.assertTrue(m.getKeys().contains("a"));
  }

  @Test
  public void testParentWithChildShadowingRemove() {
    SimpleMap<String, String> m = new InheritableSimpleMap<String, String>();
    m.put("a", "A");
    m = new InheritableSimpleMap<String, String>(m);
    Assert.assertEquals("A", m.get("a"));
    m.put("a", "B");
    Assert.assertEquals("B", m.get("a"));
    Assert.assertTrue(m.hasKey("a"));
    Assert.assertEquals(1, m.getKeys().size());
    Assert.assertTrue(m.getKeys().contains("a"));
    m.remove("a");
    Assert.assertFalse(m.hasKey("a"));
    Assert.assertTrue(m.getKeys().isEmpty());
    Assert.assertEquals(null, m.get("a"));
  }

}
