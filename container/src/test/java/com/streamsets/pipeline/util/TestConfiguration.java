/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.util;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
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
    Assert.assertEquals(Long.MAX_VALUE, conf.get("l", 2l));

    Assert.assertEquals("D", conf.get("x", "D"));
    Assert.assertEquals(false, conf.get("x", false));
    Assert.assertEquals(1, conf.get("x", 1));
    Assert.assertEquals(2l, conf.get("x", 2l));

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

  @After
  public void cleanUp() {
    Configuration.setFileRefsBaseDir(null);
  }

  @Test
  public void testFileRefs() throws IOException {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    Configuration.setFileRefsBaseDir(dir);

    Writer writer = new FileWriter(new File(dir, "hello.txt"));
    IOUtils.write("secret", writer);
    writer.close();
    Configuration conf = new Configuration();

    conf.set("a", "@hello.txt@");
    Assert.assertEquals("secret", conf.get("a", null));

    writer = new FileWriter(new File(dir, "config.properties"));
    conf.save(writer);
    writer.close();

    conf = new Configuration();
    Reader reader = new FileReader(new File(dir, "config.properties"));
    conf.load(reader);
    reader.close();

    Assert.assertEquals("secret", conf.get("a", null));

    reader = new FileReader(new File(dir, "config.properties"));
    StringWriter stringWriter = new StringWriter();
    IOUtils.copy(reader, stringWriter);
    reader.close();
    Assert.assertTrue(stringWriter.toString().contains("@hello.txt@"));
    Assert.assertFalse(stringWriter.toString().contains("secret"));
  }

  @Test(expected = RuntimeException.class)
  public void testFileRefsNotConfigured() throws IOException {
    Configuration.setFileRefsBaseDir(null);
    Configuration conf = new Configuration();
    conf.set("a", "@hello.txt@");
  }

}