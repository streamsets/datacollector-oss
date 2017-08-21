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
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

public class TestAsynchronousFileFinder {


  private void testFindAndForget(SafeScheduledExecutorService executor) throws Exception {
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

    AsynchronousFileFinder ff = new AsynchronousFileFinder(
        Paths.get(baseDir.getAbsolutePath(), "*/*.txt"),
        1,
        executor,
        FileFilterOption.FILTER_REGULAR_FILES_ONLY
    );

    try {
      Thread.sleep(2000);
      Assert.assertEquals(expected, ff.find());
      Assert.assertTrue(ff.find().isEmpty());

      File file5 = new File(nestedDir1, "a.txt");

      //forget a file we've never seen
      Assert.assertFalse(ff.forget(file5.toPath()));

      Files.createFile(file5.toPath());

      expected = ImmutableSet.of(file5.toPath());
      Thread.sleep(2000);
      Assert.assertEquals(expected, ff.find());
      Assert.assertTrue(ff.find().isEmpty());

      //forget a file we've seen
      Assert.assertTrue(ff.forget(file1.toPath()));

      //forgotten file must show up again
      expected = ImmutableSet.of(file1.toPath());
      Thread.sleep(2000);
      Assert.assertEquals(expected, ff.find());
      Assert.assertTrue(ff.find().isEmpty());
    } finally {
      if (ff != null) {
        ff.close();
        if (executor == null) {
          Assert.assertTrue(ff.getExecutor().isShutdown());
        } else {
          Assert.assertFalse(ff.getExecutor().isShutdown());
        }
      }
    }
  }

  @Test
  public void testFindAndForgetOwnExecutor() throws Exception {
    testFindAndForget(null);
  }

  @Test
  public void testFindAndForgetExternalExecutor() throws Exception {
    ScheduledExecutorService executor = new SafeScheduledExecutorService(1, "FileFinder");
    try {
      testFindAndForget(null);
    } finally {
      executor.shutdownNow();
    }
  }

}
