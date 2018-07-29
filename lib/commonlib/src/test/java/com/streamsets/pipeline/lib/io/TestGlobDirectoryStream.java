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
package com.streamsets.pipeline.lib.io;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

public class TestGlobDirectoryStream {

  @Test(expected = IOException.class)
  public void testDirDoesNotExists() throws IOException {
    File dir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    try (DirectoryStream<Path> ds = new GlobDirectoryStream(dir.toPath(), Paths.get("x.txt"))) {
      Assert.fail();
    }
  }

  @Test
  public void testDirEmpty() throws IOException {
    File dir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(dir.mkdirs());
    try (DirectoryStream<Path> ds = new GlobDirectoryStream(dir.toPath(), Paths.get("x.txt"))) {
      Assert.assertFalse(ds.iterator().hasNext());
    }
  }

  @Test
  public void testExactDirWithNoMatchingFile() throws IOException {
    File dir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    File file = new File(dir, "y.txt");
    Assert.assertTrue(dir.mkdirs());
    Files.createFile(file.toPath());
    try (DirectoryStream<Path> ds = new GlobDirectoryStream(dir.toPath(), Paths.get("x.txt"))) {
      Assert.assertFalse(ds.iterator().hasNext());
    }
  }

  @Test
  public void testExactDirWithMatchingFile() throws IOException {
    File dir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    File file1 = new File(dir, "x.txt");
    File file2 = new File(dir, "y.txt");
    Assert.assertTrue(dir.mkdirs());
    Files.createFile(file1.toPath());
    Files.createFile(file2.toPath());
    try (DirectoryStream<Path> ds = new GlobDirectoryStream(dir.toPath(), Paths.get("x.txt"))) {
      Iterator<Path> it = ds.iterator();
      Assert.assertTrue(it.hasNext());
      Assert.assertEquals(file1.toPath(), it.next());
      Assert.assertFalse(it.hasNext());
    }
  }

  @Test
  public void testWildcardDirWithNoMatchingFile() throws IOException {
    File dir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    File file = new File(dir, "y.txt");
    Assert.assertTrue(dir.mkdirs());
    Files.createFile(file.toPath());
    try (DirectoryStream<Path> ds = new GlobDirectoryStream(dir.toPath(), Paths.get("x.*"))) {
      Assert.assertFalse(ds.iterator().hasNext());
    }
  }

  @Test
  public void testWildcardDirWithMatchingFile() throws IOException {
    File dir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    File file1 = new File(dir, "x.txt");
    File file2 = new File(dir, "y.txt");
    Assert.assertTrue(dir.mkdirs());
    Files.createFile(file1.toPath());
    Files.createFile(file2.toPath());
    try (DirectoryStream<Path> ds = new GlobDirectoryStream(dir.toPath(), Paths.get("x.*"))) {
      Iterator<Path> it = ds.iterator();
      Assert.assertTrue(it.hasNext());
      Assert.assertEquals(file1.toPath(), it.next());
      Assert.assertFalse(it.hasNext());
    }
  }

  @Test
  public void testExactNestedDirWithNoMatchingFile() throws IOException {
    File baseDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    File nestedDir = new File(baseDir, "x");
    File file = new File(nestedDir, "y.txt");
    Assert.assertTrue(nestedDir.mkdirs());
    Files.createFile(file.toPath());
    try (DirectoryStream<Path> ds = new GlobDirectoryStream(baseDir.toPath(), Paths.get("x/x.txt"))) {
      Assert.assertFalse(ds.iterator().hasNext());
    }
  }

  @Test
  public void testExactNestedDirWithMatchingFile() throws IOException {
    File baseDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    File nestedDir = new File(baseDir, "x");
    File file = new File(nestedDir, "x.txt");
    Assert.assertTrue(nestedDir.mkdirs());
    Files.createFile(file.toPath());
    try (DirectoryStream<Path> ds = new GlobDirectoryStream(baseDir.toPath(), Paths.get("x/x.txt"))) {
      Iterator<Path> it = ds.iterator();
      Assert.assertTrue(it.hasNext());
      Assert.assertEquals(file.toPath(), it.next());
      Assert.assertFalse(it.hasNext());
    }
  }

  @Test
  public void testWildcardNestedDirWithNoMatchingFile() throws IOException {
    File baseDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    File nestedDir = new File(baseDir, "x");
    Assert.assertTrue(nestedDir.mkdirs());
    File file1 = new File(nestedDir, "x.txt");
    File file2 = new File(nestedDir, "y.txt");
    Files.createFile(file1.toPath());
    Files.createFile(file2.toPath());
    try (DirectoryStream<Path> ds = new GlobDirectoryStream(baseDir.toPath(), Paths.get("*/z.txt"))) {
      Assert.assertFalse(ds.iterator().hasNext());
    }
  }

  @Test
  public void testWildcardNestedDirWithMatchingFile() throws IOException {
    File baseDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    File nestedDir1 = new File(baseDir, "x");
    Assert.assertTrue(nestedDir1.mkdirs());
    File nestedDir2 = new File(baseDir, "y");
    Assert.assertTrue(nestedDir2.mkdirs());
    File file1 = new File(nestedDir1, "x.txt");
    File file2 = new File(nestedDir2, "y.txt");
    File file3 = new File(nestedDir2, "x.txt");
    Files.createFile(file1.toPath());
    Files.createFile(file2.toPath());
    Files.createFile(file3.toPath());
    try (DirectoryStream<Path> ds = new GlobDirectoryStream(baseDir.toPath(), Paths.get("*/x.txt"))) {
      Iterator<Path> it = ds.iterator();
      Assert.assertTrue(it.hasNext());
      Set<Path> found = new HashSet<>();
      found.add(it.next());
      Assert.assertTrue(it.hasNext());
      found.add(it.next());
      Assert.assertFalse(it.hasNext());
      Assert.assertEquals(ImmutableSet.of(file1.toPath(), file3.toPath()), found);
    }
  }

  @Test
  public void testMultiWildcardNestedDirWithMatchingFile() throws IOException {
    File baseDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    File nestedDir1 = new File(baseDir, "x");
    Assert.assertTrue(nestedDir1.mkdirs());
    File nestedDir2 = new File(baseDir, "y");
    Assert.assertTrue(nestedDir2.mkdirs());
    File file1 = new File(nestedDir1, "x.txt");
    File file2 = new File(nestedDir2, "y.txt");
    File file3 = new File(nestedDir2, "x.txt");
    File file4 = new File(nestedDir2, "x.x");
    Files.createFile(file1.toPath());
    Files.createFile(file2.toPath());
    Files.createFile(file3.toPath());
    Files.createFile(file4.toPath());
    GlobDirectoryStream dsRef;
    try (GlobDirectoryStream ds = new GlobDirectoryStream(baseDir.toPath(), Paths.get("*/*.txt"))) {
      dsRef = ds;
      Iterator<Path> it = ds.iterator();
      Assert.assertTrue(it.hasNext());
      Set<Path> found = new HashSet<>();
      found.add(it.next());
      Assert.assertTrue(it.hasNext());
      found.add(it.next());
      Assert.assertTrue(it.hasNext());
      found.add(it.next());
      Assert.assertFalse(it.hasNext());
      Assert.assertEquals(ImmutableSet.of(file1.toPath(), file2.toPath(), file3.toPath()), found);
    }
    Assert.assertEquals(0, dsRef.getOpenCounter());
  }

  @Test
  public void testFilter() throws IOException {
    File baseDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    File nestedDir1 = new File(baseDir, "x");
    Assert.assertTrue(nestedDir1.mkdirs());
    File nestedDir2 = new File(baseDir, "y");
    Assert.assertTrue(nestedDir2.mkdirs());
    File file1 = new File(nestedDir1, "x.txt");
    final File file2 = new File(nestedDir2, "y.txt");
    File file3 = new File(nestedDir2, "x.txt");
    File file4 = new File(nestedDir2, "x.x");
    File dir5 = new File(nestedDir2, "d.txt"); // while i matches, because it is a dir, we ignore it
    Assert.assertTrue(dir5.mkdirs());
    Files.createFile(file1.toPath());
    Files.createFile(file2.toPath());
    Files.createFile(file3.toPath());
    Files.createFile(file4.toPath());
    GlobDirectoryStream dsRef;
    DirectoryStream.Filter<Path> filter = new DirectoryStream.Filter<Path>() {
      @Override
      public boolean accept(Path entry) throws IOException {
        return entry.equals(file2.toPath());
      }
    };
    try (GlobDirectoryStream ds = new GlobDirectoryStream(baseDir.toPath(), Paths.get("*/*.txt"), filter)) {
      dsRef = ds;
      Iterator<Path> it = ds.iterator();
      Assert.assertTrue(it.hasNext());
      Set<Path> found = new HashSet<>();
      found.add(it.next());
      Assert.assertFalse(it.hasNext());
      Assert.assertEquals(ImmutableSet.of(file2.toPath()), found);
    }
    Assert.assertEquals(0, dsRef.getOpenCounter());
  }

  @Test
  public void testNoOpenDirectoryStreams() throws IOException {
    File baseDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    File nestedDir1 = new File(baseDir, "x");
    Assert.assertTrue(nestedDir1.mkdirs());
    File nestedDir2 = new File(baseDir, "y");
    Assert.assertTrue(nestedDir2.mkdirs());
    File file1 = new File(nestedDir1, "x.txt");
    File file2 = new File(nestedDir2, "y.txt");
    File file3 = new File(nestedDir2, "x.txt");
    File file4 = new File(nestedDir2, "x.x");
    Files.createFile(file1.toPath());
    Files.createFile(file2.toPath());
    Files.createFile(file3.toPath());
    Files.createFile(file4.toPath());
    GlobDirectoryStream dsRef;
    try (GlobDirectoryStream ds = new GlobDirectoryStream(baseDir.toPath(), Paths.get("*/*.txt"))) {
      dsRef = ds;
      Iterator<Path> it = ds.iterator();
      it.hasNext();
      it.next();
    }
    Assert.assertEquals(0, dsRef.getOpenCounter());
  }

}
