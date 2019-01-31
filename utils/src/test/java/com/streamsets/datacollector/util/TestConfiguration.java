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
package com.streamsets.datacollector.util;

import com.google.common.collect.ImmutableSet;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.UUID;

public class TestConfiguration {

  @Before
  @After
  public void cleanUp() {
    Configuration.setFileRefsBaseDir(null);
  }

  @Test
  public void testBasicMethods() {
    Configuration conf = new Configuration();

    Assert.assertTrue(conf.getNames().isEmpty());

    conf.set("s", "S");
    conf.set("b", true);
    conf.set("i", Integer.MAX_VALUE);
    conf.set("l", Long.MAX_VALUE);

    Assert.assertTrue(conf.hasName("s"));

    Assert.assertEquals(ImmutableSet.of("s", "b", "i", "l"), conf.getNames());
    Assert.assertEquals("S", conf.get("s", "D"));
    Assert.assertEquals(true, conf.get("b", false));
    Assert.assertEquals(Integer.MAX_VALUE, conf.get("i", 1));
    Assert.assertEquals(Long.MAX_VALUE, conf.get("l", 2L));

    Assert.assertEquals("D", conf.get("x", "D"));
    Assert.assertEquals(false, conf.get("x", false));
    Assert.assertEquals(1, conf.get("x", 1));
    Assert.assertEquals(2L, conf.get("x", 2L));

    conf.unset("s");
    Assert.assertFalse(conf.hasName("s"));
    Assert.assertEquals(null, conf.get("s", null));
  }

  @Test(expected = NullPointerException.class)
  public void invalidCall1() {
    Configuration conf = new Configuration();
    conf.set(null, null);
  }

  @Test(expected = NullPointerException.class)
  public void invalidCall2() {
    Configuration conf = new Configuration();
    conf.set("a", null);
  }

  @Test(expected = NullPointerException.class)
  public void invalidCall3() {
    Configuration conf = new Configuration();
    conf.unset(null);
  }

  @Test(expected = NullPointerException.class)
  public void invalidCall4() {
    Configuration conf = new Configuration();
    conf.hasName(null);
  }

  @Test(expected = NullPointerException.class)
  public void invalidCall5() {
    Configuration conf = new Configuration();
    conf.getSubSetConfiguration(null);
  }

  @Test(expected = NullPointerException.class)
  public void invalidCall6() throws IOException {
    Configuration conf = new Configuration();
    conf.load(null);
  }

  @Test(expected = NullPointerException.class)
  public void invalidCall7() throws IOException {
    Configuration conf = new Configuration();
    conf.save(null);
  }

  @Test
  public void testSubSet() {
    Configuration conf = new Configuration();

    conf.set("a.s", "S");
    conf.set("a.b", true);
    conf.set("x.i", Integer.MAX_VALUE);
    conf.set("x.l", Long.MAX_VALUE);

    Configuration subSet = conf.getSubSetConfiguration("a.");

    Assert.assertEquals(ImmutableSet.of("a.s", "a.b", "x.i", "x.l"), conf.getNames());

    Assert.assertEquals(ImmutableSet.of("a.s", "a.b"), subSet.getNames());

  }

  @Test
  public void testSubSetDropPrefix() {
    Configuration conf = new Configuration();

    conf.set("a.s", "S");
    conf.set("a.b", true);
    conf.set("x.i", Integer.MAX_VALUE);
    conf.set("x.l", Long.MAX_VALUE);

    Configuration subSet = conf.getSubSetConfiguration("a.", true);
    Assert.assertEquals(ImmutableSet.of("s", "b"), subSet.getNames());

  }

  @Test
  public void testFilterSensitive() {
    Configuration conf = new Configuration();

    conf.set("realPassword", "secret password");

    Configuration redacted = conf.maskSensitiveConfigs();
    Assert.assertEquals(Configuration.SENSITIVE_MASK, redacted.get("realPassword", null));
  }

  @Test
  public void testSaveLoad() throws IOException {
    Configuration conf = new Configuration();

    conf.set("a.s", "S");
    conf.set("a.b", true);
    conf.set("x.i", Integer.MAX_VALUE);
    conf.set("x.l", Long.MAX_VALUE);

    StringWriter writer = new StringWriter();
    conf.save(writer);

    StringReader reader = new StringReader(writer.toString());

    conf = new Configuration();
    conf.set("a.s", "X");
    conf.set("a.ss", "XX");

    conf.load(reader);

    Assert.assertEquals(ImmutableSet.of("a.s", "a.b", "x.i", "x.l", "a.ss"), conf.getNames());

    Assert.assertEquals("S", conf.get("a.s", "D"));
    Assert.assertEquals("XX", conf.get("a.ss", "D"));

  }

  @Test
  public void testFileRefs() throws IOException {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    Configuration.setFileRefsBaseDir(dir);

    Writer writer = new FileWriter(new File(dir, "hello.txt"));
    IOUtils.write("secret\nfoo\n", writer);
    writer.close();
    Configuration conf = new Configuration();

    conf.set("a", "@hello.txt@");
    Assert.assertEquals("secret\nfoo\n", conf.get("a", null));

    conf.set("aa", "${file(\"hello.txt\")}");
    Assert.assertEquals("secret\nfoo\n", conf.get("aa", null));

    conf.set("aaa", "${file('hello.txt')}");
    Assert.assertEquals("secret\nfoo\n", conf.get("aaa", null));

    writer = new FileWriter(new File(dir, "config.properties"));
    conf.save(writer);
    writer.close();

    conf = new Configuration();
    Reader reader = new FileReader(new File(dir, "config.properties"));
    conf.load(reader);
    reader.close();

    Assert.assertEquals("secret\nfoo\n", conf.get("a", null));

    reader = new FileReader(new File(dir, "config.properties"));
    StringWriter stringWriter = new StringWriter();
    IOUtils.copy(reader, stringWriter);
    reader.close();
    Assert.assertTrue(stringWriter.toString().contains("@hello.txt@"));
    Assert.assertTrue(stringWriter.toString().contains("${file(\"hello.txt\")}"));
    Assert.assertTrue(stringWriter.toString().contains("${file('hello.txt')}"));
    Assert.assertFalse(stringWriter.toString().contains("secret\nfoo\n"));
  }

  @Test(expected = RuntimeException.class)
  public void testFileRefsNotConfigured() throws IOException {
    Configuration.setFileRefsBaseDir(null);
    Configuration conf = new Configuration();
    conf.set("a", "@hello.txt@");
  }

  @Test(expected = RuntimeException.class)
  public void testNewFileRefsNotConfigured() throws IOException {
    Configuration.setFileRefsBaseDir(null);
    Configuration conf = new Configuration();
    conf.set("a", "${file(\"hello.txt\")}");
  }

  @Test
  public void testRefsConfigs() throws IOException {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    Configuration.setFileRefsBaseDir(dir);

    Writer writer = new FileWriter(new File(dir, "hello.txt"));
    IOUtils.write("secret", writer);
    writer.close();
    Configuration conf = new Configuration();

    String home = System.getenv("HOME");

    conf.set("a", "@hello.txt@");
    conf.set("aa", "${file(\"hello.txt\")}");
    conf.set("aaa", "${file('hello.txt')}");
    conf.set("b", "$HOME$");
    conf.set("bb", "${env(\"HOME\")}");
    conf.set("bbb", "${env('HOME')}");
    conf.set("x", "X");
    Assert.assertEquals("secret", conf.get("a", null));
    Assert.assertEquals("secret", conf.get("aa", null));
    Assert.assertEquals("secret", conf.get("aaa", null));
    Assert.assertEquals(home, conf.get("b", null));
    Assert.assertEquals(home, conf.get("bb", null));
    Assert.assertEquals(home, conf.get("bbb", null));
    Assert.assertEquals("X", conf.get("x", null));

    Configuration uconf = conf.getUnresolvedConfiguration();
    Assert.assertEquals("@hello.txt@", uconf.get("a", null));
    Assert.assertEquals("${file(\"hello.txt\")}", uconf.get("aa", null));
    Assert.assertEquals("${file('hello.txt')}", uconf.get("aaa", null));
    Assert.assertEquals("$HOME$", uconf.get("b", null));
    Assert.assertEquals("${env(\"HOME\")}", uconf.get("bb", null));
    Assert.assertEquals("${env('HOME')}", uconf.get("bbb", null));
    Assert.assertEquals("X", uconf.get("x", null));

    writer = new FileWriter(new File(dir, "config.properties"));
    conf.save(writer);
    writer.close();

    conf = new Configuration();
    Reader reader = new FileReader(new File(dir, "config.properties"));
    conf.load(reader);
    reader.close();

    uconf = conf.getUnresolvedConfiguration();
    Assert.assertEquals("@hello.txt@", uconf.get("a", null));
    Assert.assertEquals("${file(\"hello.txt\")}", uconf.get("aa", null));
    Assert.assertEquals("${file('hello.txt')}", uconf.get("aaa", null));
    Assert.assertEquals("$HOME$", uconf.get("b", null));
    Assert.assertEquals("${env(\"HOME\")}", uconf.get("bb", null));
    Assert.assertEquals("${env('HOME')}", uconf.get("bbb", null));
    Assert.assertEquals("X", uconf.get("x", null));
  }

  @Test
  public void testIncludes() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    Configuration.setFileRefsBaseDir(dir);

    Writer writer = new FileWriter(new File(dir, "config.properties"));
    IOUtils.write("a=A\nconfig.includes=include1.properties , ", writer);
    writer.close();

    writer = new FileWriter(new File(dir, "include1.properties"));
    IOUtils.write("b=B\nconfig.includes=include2.properties , ", writer);
    writer.close();

    writer = new FileWriter(new File(dir, "include2.properties"));
    IOUtils.write("c=C\n", writer);
    writer.close();

    Configuration conf = new Configuration();
    Reader reader = new FileReader(new File(dir, "config.properties"));
    conf.load(reader);
    reader.close();

    Assert.assertEquals("A", conf.get("a", null));
    Assert.assertEquals("B", conf.get("b", null));
    Assert.assertEquals("C", conf.get("c", null));
    Assert.assertNull(conf.get(Configuration.CONFIG_INCLUDES, null));
  }

  private void testExecRefOk(boolean absolutePath) throws IOException {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    Configuration.setFileRefsBaseDir(dir);

    File script;
    String scriptForConfig;
    if (absolutePath) {
      dir = new File("target", UUID.randomUUID().toString());
      Assert.assertTrue(dir.mkdirs());
      script = new File(dir, "script.sh").getAbsoluteFile();
      scriptForConfig = script.getAbsolutePath();
    } else {
      script = new File(dir, "script.sh");
      scriptForConfig = script.getName();
    }

    try (Writer writer = new FileWriter(script)) {
      writer.write("#!/bin/bash\necho -n \"secret\"\necho \"error\" >&2");
    }
    script.setExecutable(true);

    Configuration conf = new Configuration();
    conf.set("a", "${exec(\""+ scriptForConfig +"\")}");
    Assert.assertEquals("secret", conf.get("a", null));

    try (Writer writer = new FileWriter(new File(dir, "config.properties"))) {
      conf.save(writer);
    }

    conf = new Configuration();
    try (Reader reader = new FileReader(new File(dir, "config.properties"))) {
      conf.load(reader);
    }

    Assert.assertEquals("secret", conf.get("a", null));

    try (Reader reader = new FileReader(new File(dir, "config.properties"))) {
      StringWriter stringWriter = new StringWriter();
      IOUtils.copy(reader, stringWriter);
      Assert.assertTrue(stringWriter.toString().contains("a=${exec(\"" + scriptForConfig + "\")}"));
    }
  }

  @Test
  public void testExecRefRelativeOk() throws IOException {
    testExecRefOk(false);
  }

  @Test
  public void testExecRefAbsoluteOk() throws IOException {
    testExecRefOk(true);
  }

  @Test(expected = Configuration.ConfigurationException.class)
  public void testExecRefScriptNotFoundError() {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    Configuration.setFileRefsBaseDir(dir);

    Configuration conf = new Configuration();
    conf.set("a", "${exec(\"non-existent.sh\")}");
    conf.get("a", null);
  }

  @Test(expected = Configuration.ConfigurationException.class)
  public void testExecRefScriptNotExecutableError() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    Configuration.setFileRefsBaseDir(dir);

    File script = new File(dir, "script.sh");

    try (Writer writer = new FileWriter(script)) {
      writer.write("#!/bin/bash\necho -n \"secret\"\necho \"error\" >&2");
    }

    Configuration conf = new Configuration();
    conf.set("a", "${exec(\"script.sh\")}");
    conf.get("a", null);
  }

  @Test(expected = Configuration.ConfigurationException.class)
  public void testExecRefScriptOverflowError() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    Configuration.setFileRefsBaseDir(dir);

    File script = new File(dir, "script.sh");

    try (Writer writer = new FileWriter(script)) {
      writer.write("#!/bin/bash\nwhile true; do echo -n \"secret\"; done");
    }
    script.setExecutable(true);

    Configuration conf = new Configuration();
    conf.set("a", "${exec(\"script.sh\")}");
    conf.get("a", null);
  }


  @Test(expected = Configuration.ConfigurationException.class)
  public void testExecRefScriptNonZeroExitError() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    Configuration.setFileRefsBaseDir(dir);

    File script = new File(dir, "script.sh");

    try (Writer writer = new FileWriter(script)) {
      writer.write("#!/bin/bash\nexit 1");
    }
    script.setExecutable(true);

    Configuration conf = new Configuration();
    conf.set("a", "${exec(\"script.sh\")}");
    conf.get("a", null);
  }
}