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
package com.streamsets.pipeline.lib.io;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.UUID;

public class TestSynchronousFileFinder {

  @Test(expected = IllegalArgumentException.class)
  public void testDoubleStarWildcard() {
    new SynchronousFileFinder(Paths.get("/foo/**/x.txt"));
  }

    @Test
  public void testHasWildcard() {
    Assert.assertFalse(SynchronousFileFinder.hasGlobWildcard(""));
    Assert.assertFalse(SynchronousFileFinder.hasGlobWildcard("a"));
    Assert.assertFalse(SynchronousFileFinder.hasGlobWildcard("\\*"));
    Assert.assertFalse(SynchronousFileFinder.hasGlobWildcard("a\\?"));
    Assert.assertFalse(SynchronousFileFinder.hasGlobWildcard("\\[z"));
    Assert.assertFalse(SynchronousFileFinder.hasGlobWildcard("a\\{z"));
    Assert.assertTrue(SynchronousFileFinder.hasGlobWildcard("*"));
    Assert.assertTrue(SynchronousFileFinder.hasGlobWildcard("?"));
    Assert.assertTrue(SynchronousFileFinder.hasGlobWildcard("["));
    Assert.assertTrue(SynchronousFileFinder.hasGlobWildcard("{"));
    Assert.assertTrue(SynchronousFileFinder.hasGlobWildcard("a*"));
    Assert.assertTrue(SynchronousFileFinder.hasGlobWildcard("*z"));
    Assert.assertTrue(SynchronousFileFinder.hasGlobWildcard("a*z"));
  }

  @Test
  public void testPivotAndWildcardDetection() {
    Path path = Paths.get("/file.log");
    SynchronousFileFinder ff = new SynchronousFileFinder(path);
    Assert.assertEquals(path, ff.getPivotPath());
    Assert.assertEquals(null, ff.getWildcardPath());

    path = Paths.get("/xx/file.log");
    ff = new SynchronousFileFinder(path);
    Assert.assertEquals(path, ff.getPivotPath());
    Assert.assertEquals(null, ff.getWildcardPath());

    path = Paths.get("/*/file.log");
    ff = new SynchronousFileFinder(path);
    Assert.assertEquals(Paths.get("/"), ff.getPivotPath());
    Assert.assertEquals(Paths.get("*/file.log"), ff.getWildcardPath());

    path = Paths.get("/x/*/file.log");
    ff = new SynchronousFileFinder(path);
    Assert.assertEquals(Paths.get("/x"), ff.getPivotPath());
    Assert.assertEquals(Paths.get("*/file.log"), ff.getWildcardPath());

    path = Paths.get("/y/x/*/*file.log");
    ff = new SynchronousFileFinder(path);
    Assert.assertEquals(Paths.get("/y/x"), ff.getPivotPath());
    Assert.assertEquals(Paths.get("*/*file.log"), ff.getWildcardPath());

    path = Paths.get("/x/\\*/file.log");
    ff = new SynchronousFileFinder(path);
    Assert.assertEquals(path, ff.getPivotPath());
    Assert.assertEquals(null, ff.getWildcardPath());
  }


  @Test
  public void testFindAndForget() throws IOException {
    File baseDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    File nestedDir1 = new File(baseDir, "x");
    Assert.assertTrue(nestedDir1.mkdirs());
    File nestedDir2 = new File(baseDir, "y");
    Assert.assertTrue(nestedDir2.mkdirs());
    File file1 = new File(nestedDir1, "x.txt");
    final File file2 = new File(nestedDir2, "y.txt");
    File file3 = new File(nestedDir2, "x.txt");
    File file4 = new File(nestedDir2, "x.x");
    File dir5 = new File(nestedDir2, "d.txt");
    Assert.assertTrue(dir5.mkdirs());
    Files.createFile(file1.toPath());
    Files.createFile(file2.toPath());
    Files.createFile(file3.toPath());
    Files.createFile(file4.toPath());

    Set<Path> expected = ImmutableSet.of(file1.toPath(), file2.toPath(), file3.toPath());

    FileFinder ff = new SynchronousFileFinder(Paths.get(baseDir.getAbsolutePath(), "*/*.txt"));
    Assert.assertEquals(expected, ff.find());
    Assert.assertTrue(ff.find().isEmpty());

    File file5 = new File(nestedDir1, "a.txt");
    Files.createFile(file5.toPath());

    //forget a file we've never seen
    Assert.assertFalse(ff.forget(file5.toPath()));

    expected = ImmutableSet.of(file5.toPath());
    Assert.assertEquals(expected, ff.find());
    Assert.assertTrue(ff.find().isEmpty());

    //forget a file we've seen
    Assert.assertTrue(ff.forget(file1.toPath()));

    //forgotten file must show up again
    expected = ImmutableSet.of(file1.toPath());
    Assert.assertEquals(expected, ff.find());
    Assert.assertTrue(ff.find().isEmpty());

  }

}