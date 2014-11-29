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
package com.streamsets.pipeline.util;

import com.streamsets.pipeline.container.LocaleInContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Locale;

public class TestMessage {

  @After
  public void cleanUp() {
    LocaleInContext.set(null);
  }

  @Test(expected = NullPointerException.class)
  public void testConstructorFail1() {
    new Message(null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testConstructorFail2() {
    new Message("key", null);
  }

  @Test(expected = NullPointerException.class)
  public void testConstructorFail3() {
    new Message((ClassLoader)null, null, "key", "default '{}'", "hello");
  }

  @Test(expected = NullPointerException.class)
  public void testConstructorFail4() {
    new Message(getClass().getClassLoader(), null, "key", "default '{}'", "hello");
  }

  @Test
  public void testConstructorDefaultClassLoaderBundle() {
    new Message("key", "default '{}'");
    new Message("key", "default '{}'", null);
    new Message("key", "default '{}'", "hello");
  }

  @Test
  public void testConstructorCustomClassLoaderBundle() {
    new Message(getClass().getClassLoader(), "test-bundle", "key", "default '{}'");
    new Message(getClass().getClassLoader(), "test-bundle", "key", "default '{}'", null);
    new Message(getClass().getClassLoader(), "test-bundle", "key", "default '{}'", "hello");
  }

  @Test
  public void testNoBundle() {
    Message msg = new Message(getClass().getClassLoader(), "missing-bundle", "key", "default '{}'", "foo");
    Assert.assertEquals("default 'foo'", msg.getMessage());
  }

  @Test
  public void testNoKey() {
    Message msg = new Message(getClass().getClassLoader(), "test-message-bundle", "missing-key", "default '{}'", "foo");
    Assert.assertEquals("default 'foo'", msg.getMessage());
  }

  @Test
  public void testDefaultBundle() {
    LocaleInContext.set(Locale.ENGLISH);
    Message msg = new Message("key", "default");
    Assert.assertEquals("en", msg.getMessage());
    Assert.assertTrue(msg.toString().endsWith("default"));
  }


  @Test
  public void testLocale() {
    LocaleInContext.set(Locale.ENGLISH);
    Message msg = new Message(getClass().getClassLoader(), "test-message-bundle", "key", "default '{}'", "foo");
    Assert.assertEquals("bundle 'foo'", msg.getMessage());
    Assert.assertTrue(msg.toString().endsWith("default 'foo'"));
  }

  @Test
  public void testMissingArgs() {
    LocaleInContext.set(Locale.ENGLISH);
    Message msg = new Message(getClass().getClassLoader(), "test-message-bundle", "key", "default '{}'");
    Assert.assertEquals("bundle '{}'", msg.getMessage());
    Assert.assertTrue(msg.toString().endsWith("default '{}'"));
  }

}
