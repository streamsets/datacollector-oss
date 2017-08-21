/*
 * Copyright 2017 StreamSets Inc.
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
    Assert.assertEquals("BlackListURLClassLoader[type=test name=foo]", cl.toString());
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

  public static File getBaseDir() {
    URL dummyResource = TestBlackListURLClassLoader.class.getClassLoader().getResource("dummy-resource.properties");
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
