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

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.streamsets.pipeline.api.impl.Utils;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestSystemProcess {

  private File tempDir;

  private SystemProcessImpl process;

  @Before
  public void setup() throws Exception {
    tempDir = Files.createTempDir();
  }
  @After
  public void tearDown() {
    if (process != null) {
      process.cleanup();
    }
    if (tempDir != null) {
      FileUtils.deleteQuietly(tempDir);
    }
  }


  @Test
  public void testStart() throws Exception {
    process = new SystemProcessImpl("sleep", tempDir, Arrays.
      asList("/bin/sleep", "1.1"));
    process.start();
    Assert.assertTrue(process.isAlive());
  }
  @Test
  public void testKill() throws Exception {
    process = new SystemProcessImpl("sleep", tempDir, Arrays.
      asList("/bin/sleep", "3.1"));
    process.start();
    long start = System.currentTimeMillis();
    process.kill(5000);
    long elapsed = System.currentTimeMillis() - start;
    Assert.assertTrue("Expected elapsed time to be less than 3000: " + elapsed,
      elapsed < 3000);
  }
  @Test
  public void testForceKill() throws Exception {
    Assume.assumeFalse("Test only works on java 1.8 and newer, not: " + System.getProperty("java.version"),
      System.getProperty("java.version", "").contains("1.7"));
    Assume.assumeTrue("Test only works on linux, not: " + System.getProperty("os.name"),
      System.getProperty("os.name", "").trim().toLowerCase().contains("linux"));
    process = new SystemProcessImpl("sleep", tempDir, Arrays.
      asList("/bin/bash", "-c", "trap true 15; sleep 3.1"));
    process.start();
    long start = System.currentTimeMillis();
    process.kill(5000);
    long elapsed = System.currentTimeMillis() - start;
    Assert.assertTrue("Expected elapsed time to be less than 3000: " + elapsed,
      elapsed < 3000);
  }

  @Test
  public void testOutput() throws Exception {
    process = new SystemProcessImpl("output", tempDir, Arrays.
      asList("/bin/bash", "-c", "for i in {0..2}; do echo $i; done"));
    process.start();
    while(process.isAlive()) {
      TimeUnit.MILLISECONDS.sleep(10);
    }
    String error = Joiner.on("\n").join(process.getAllError());
    Assert.assertTrue(error, error.isEmpty());
    List<String> lines = new ArrayList<>();
    Iterables.addAll(lines, process.getOutput());
    Assert.assertEquals(Arrays.asList("0", "1", "2"), lines);
  }

  @Test
  public void testError() throws Exception {
    process = new SystemProcessImpl("output", tempDir, Arrays.
      asList("/bin/bash", "-c", "for i in {0..2}; do echo $i 1>&2; done"));
    process.start();
    while(process.isAlive()) {
      TimeUnit.MILLISECONDS.sleep(10);
    }
    String error = Joiner.on("\n").join(process.getAllOutput());
    Assert.assertTrue(error, error.isEmpty());
    List<String> lines = new ArrayList<>();
    Iterables.addAll(lines, process.getError());
    Assert.assertEquals(Arrays.asList("0", "1", "2"), lines);
  }

  @Test
  public void testCleanup() throws Exception {
    for (int i = 0; i < 10; i++) {
      File f = new File(tempDir, Utils.format("file-{}{}", i, SystemProcessImpl.OUT_EXT));
      Assert.assertTrue(f.createNewFile());
    }
    SystemProcessImpl.clean(tempDir, 5);
    String[] expected = tempDir.list();
    Arrays.sort(expected);
    Assert.assertArrayEquals(new String[]{"file-5.out", "file-6.out", "file-7.out", "file-8.out", "file-9.out"},
      expected);
  }
}
