/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.record;

import org.junit.Assert;
import org.junit.Test;

public class TestVersionedSimpleMap {

  @Test
  public void testNoParent() {
    SimpleMap<String, String> m = new VersionedSimpleMap<String, String>();
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
    SimpleMap<String, String> m = new VersionedSimpleMap<String, String>();
    m = new VersionedSimpleMap<String, String>(m);
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
    SimpleMap<String, String> m = new VersionedSimpleMap<String, String>();
    m.put("a", "A");
    m = new VersionedSimpleMap<String, String>(m);
    Assert.assertEquals("A", m.get("a"));
    Assert.assertTrue(m.hasKey("a"));
    Assert.assertTrue(m.getKeys().contains("a"));
    Assert.assertEquals(1, m.getKeys().size());
  }

  @Test
  public void testParentWithChildNoShadowing() {
    SimpleMap<String, String> m = new VersionedSimpleMap<String, String>();
    m.put("b", "B");
    m = new VersionedSimpleMap<String, String>(m);
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
    SimpleMap<String, String> m = new VersionedSimpleMap<String, String>();
    m.put("a", "A");
    m = new VersionedSimpleMap<String, String>(m);
    Assert.assertEquals("A", m.get("a"));
    m.put("a", "B");
    Assert.assertEquals("B", m.get("a"));
    Assert.assertTrue(m.hasKey("a"));
    Assert.assertEquals(1, m.getKeys().size());
    Assert.assertTrue(m.getKeys().contains("a"));
  }

  @Test
  public void testParentWithChildShadowingRemove() {
    SimpleMap<String, String> m = new VersionedSimpleMap<String, String>();
    m.put("a", "A");
    m = new VersionedSimpleMap<String, String>(m);
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
