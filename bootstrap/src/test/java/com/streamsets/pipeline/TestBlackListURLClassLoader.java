/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;

public class TestBlackListURLClassLoader {

  @Test
  @SuppressWarnings("unchecked")
  public void testToStringAndName() {
    BlackListURLClassLoader cl = new BlackListURLClassLoader("test", "foo", Collections.EMPTY_LIST, getClass().getClassLoader(),
                                                             null);
    Assert.assertTrue(cl.toString().startsWith("BlackListURLClassLoader test 'foo' :"));
    Assert.assertEquals("foo", cl.getName());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testValidateClass1() {
    BlackListURLClassLoader cl = new BlackListURLClassLoader("test", "", Collections.EMPTY_LIST, getClass().getClassLoader(),
                                                             null);
    cl.validateClass("x.x.X");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testValidateClass2() {
    BlackListURLClassLoader cl = new BlackListURLClassLoader("test", "", Collections.EMPTY_LIST, getClass().getClassLoader(),
                                                             new String[] { "a.b.", "c.d."});
    cl.validateClass("x.x.X");
  }

  @Test(expected = IllegalArgumentException.class)
  @SuppressWarnings("unchecked")
  public void testValidateClassError1() {
    BlackListURLClassLoader cl = new BlackListURLClassLoader("test", "", Collections.EMPTY_LIST, getClass().getClassLoader(),
                                                             new String[] { "a.b.", "c.d."});
    cl.validateClass("a.b.X");
  }

  @Test(expected = IllegalArgumentException.class)
  @SuppressWarnings("unchecked")
  public void testValidateClassError2() {
    BlackListURLClassLoader cl = new BlackListURLClassLoader("test", "", Collections.EMPTY_LIST, getClass().getClassLoader(),
                                                             new String[] { "a.b.", "c.d."});
    cl.validateClass("c.d.X");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testValidateResource1() {
    BlackListURLClassLoader cl = new BlackListURLClassLoader("test", "", Collections.EMPTY_LIST, getClass().getClassLoader(),
                                                             new String[] { "a.b.", "c.d."});
    cl.validateResource("/x/x/X");
    cl.validateResource("x/x/X");
    cl.validateResource("x/x/X.properties");
  }

  @Test(expected = IllegalArgumentException.class)
  @SuppressWarnings("unchecked")
  public void testValidateResourceError1() {
    BlackListURLClassLoader cl = new BlackListURLClassLoader("test", "", Collections.EMPTY_LIST, getClass().getClassLoader(),
                                                             new String[] { "a.b.", "c.d."});
    cl.validateResource("a/b/X");
  }

  @Test(expected = IllegalArgumentException.class)
  @SuppressWarnings("unchecked")
  public void testValidateResourceError2() {
    BlackListURLClassLoader cl = new BlackListURLClassLoader("test", "", Collections.EMPTY_LIST, getClass().getClassLoader(),
                                                             new String[] { "a.b.", "c.d."});
    cl.validateResource("c/d/X");
  }

  @Test(expected = IllegalArgumentException.class)
  @SuppressWarnings("unchecked")
  public void testValidateResourceError3() {
    BlackListURLClassLoader cl = new BlackListURLClassLoader("test", "", Collections.EMPTY_LIST, getClass().getClassLoader(),
                                                             new String[] { "a.b.", "c.d."});
    cl.validateResource("c/d/X.properties");
  }

  private File getBaseDir() {
    URL dummyResource = getClass().getClassLoader().getResource("dummy-resource.properties");
    Assert.assertNotNull(dummyResource);
    String path = dummyResource.toExternalForm();
    Assert.assertTrue(path.startsWith("file:"));
    path = path.substring("file:".length());
    if (path.startsWith("///")) {
      path = path.substring(2);
    }
    return new File(new File(path).getParentFile(), "base-dir");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFindResource() throws Exception {
    File dir = getBaseDir();
    BlackListURLClassLoader cl = new BlackListURLClassLoader("test", "", Arrays.asList(dir.toURI().toURL()),
                                                             getClass().getClassLoader(), new String[] { "a.b."});
    Assert.assertNotNull(cl.getResource("x/y/resource.properties"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFindResources() throws Exception {
    File dir = getBaseDir();
    BlackListURLClassLoader cl = new BlackListURLClassLoader("test", "", Arrays.asList(dir.toURI().toURL()),
                                                             getClass().getClassLoader(), new String[] { "a.b."});
    Assert.assertNotNull(cl.getResources("x/y/resource.properties").hasMoreElements());
  }

  @Test(expected = IllegalArgumentException.class)
  @SuppressWarnings("unchecked")
  public void testFindResourceBlacklisted() throws Exception {
    File dir = getBaseDir();
    BlackListURLClassLoader cl = new BlackListURLClassLoader("test", "", Arrays.asList(dir.toURI().toURL()),
                                                             getClass().getClassLoader(), new String[] { "a.b."});
    cl.getResource("a/b/resource.properties");
  }

  @Test(expected = IllegalArgumentException.class)
  @SuppressWarnings("unchecked")
  public void testFindResourcesBlacklisted() throws Exception {
    File dir = getBaseDir();
    BlackListURLClassLoader cl = new BlackListURLClassLoader("test", "", Arrays.asList(dir.toURI().toURL()),
                                                             getClass().getClassLoader(), new String[] { "a.b."});
    cl.getResources("a/b/resource.properties");
  }

  @Test(expected = IllegalArgumentException.class)
  @SuppressWarnings("unchecked")
  public void testLoadClassBlacklisted() throws Exception {
    File dir = getBaseDir();
    BlackListURLClassLoader cl = new BlackListURLClassLoader("test", "", Arrays.asList(dir.toURI().toURL()),
                                                             getClass().getClassLoader(), new String[] { "a.b."});
    Assert.assertNotNull(cl.loadClass("a.b.Dummy"));
  }

  // we are expecting a ClassFormatError because the class file is invalid
  @Test(expected = ClassFormatError.class)
  @SuppressWarnings("unchecked")
  public void testLoadClass() throws Exception {
    File dir = getBaseDir();
    BlackListURLClassLoader cl = new BlackListURLClassLoader("test", "", Arrays.asList(dir.toURI().toURL()),
                                                             getClass().getClassLoader(), new String[] { "a.b."});
    Assert.assertNotNull(cl.loadClass("x.y.Dummy"));
  }

}
