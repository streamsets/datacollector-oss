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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;

public class TestSynchronousFileFinder {

  @Test(expected = IllegalArgumentException.class)
  public void testDoubleStarWildcard() {
    new SynchronousFileFinder(Paths.get("/foo/**/x.txt"), FileFilterOption.FILTER_REGULAR_FILES_ONLY);
  }

  @Test
  public void testPivotAndWildcardDetection() {
    Path path = Paths.get("/file.log");
    SynchronousFileFinder ff = new SynchronousFileFinder(path, FileFilterOption.FILTER_REGULAR_FILES_ONLY);
    Assert.assertEquals(path, ff.getPivotPath());
    Assert.assertEquals(null, ff.getWildcardPath());

    path = Paths.get("/xx/file.log");
    ff = new SynchronousFileFinder(path, FileFilterOption.FILTER_REGULAR_FILES_ONLY);
    Assert.assertEquals(path, ff.getPivotPath());
    Assert.assertEquals(null, ff.getWildcardPath());

    path = Paths.get("/*/file.log");
    ff = new SynchronousFileFinder(path, FileFilterOption.FILTER_REGULAR_FILES_ONLY);
    Assert.assertEquals(Paths.get("/"), ff.getPivotPath());
    Assert.assertEquals(Paths.get("*/file.log"), ff.getWildcardPath());

    path = Paths.get("/x/*/file.log");
    ff = new SynchronousFileFinder(path, FileFilterOption.FILTER_REGULAR_FILES_ONLY);
    Assert.assertEquals(Paths.get("/x"), ff.getPivotPath());
    Assert.assertEquals(Paths.get("*/file.log"), ff.getWildcardPath());

    path = Paths.get("/y/x/*/*file.log");
    ff = new SynchronousFileFinder(path, FileFilterOption.FILTER_REGULAR_FILES_ONLY);
    Assert.assertEquals(Paths.get("/y/x"), ff.getPivotPath());
    Assert.assertEquals(Paths.get("*/*file.log"), ff.getWildcardPath());

    path = Paths.get("/x/\\*/file.log");
    ff = new SynchronousFileFinder(path, FileFilterOption.FILTER_REGULAR_FILES_ONLY);
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

    FileFinder ff = new SynchronousFileFinder(
        Paths.get(baseDir.getAbsolutePath(), "*/*.txt"),
        FileFilterOption.FILTER_REGULAR_FILES_ONLY
    );
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

  @Test
  public void testFindDirectoriesOnly() throws Exception {
    File baseDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    //Create the following
    //Directories - target/${uuid}/a/bc, target/${uuid}/d/ef
    Path dirPath1 = Paths.get(baseDir.getAbsolutePath().toString() + File.separatorChar + "a" + File.separatorChar + "bc");
    Path dirPath2 = Paths.get(baseDir.getAbsolutePath().toString() + File.separatorChar + "d" + File.separatorChar + "ef");

    //Files - target/${uuid}/a/file.txt, target/${uuid}/d/file.txt
    Path filePath1 = Paths.get(dirPath1.getParent().toString() + File.separatorChar + "file.txt");
    Path filePath2 = Paths.get(dirPath2.getParent().toString() + File.separatorChar + "file.txt");

    Files.createDirectories(dirPath1);
    Files.createDirectories(dirPath2);

    Files.createFile(filePath1);
    Files.createFile(filePath2);

    //Should not contain the above files.
    FileFinder ff = new SynchronousFileFinder(
        Paths.get(baseDir.getAbsolutePath(), "*/*"),
        FileFilterOption.FILTER_DIRECTORIES_ONLY
    );
    Set<Path> paths = ff.find();
    Assert.assertEquals(2L, paths.size());
    Assert.assertTrue(paths.containsAll(Arrays.asList(dirPath1, dirPath2)));
  }

  @Test
  public void testFindDirectoriesAndRegularFiles() throws Exception {
    File baseDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    //Create the following
    //Directories - target/${uuid}/a/bc, target/${uuid}/d/ef
    Path dirPath1 = Paths.get(baseDir.getAbsolutePath().toString() + File.separatorChar + "a" + File.separatorChar + "bc");
    Path dirPath2 = Paths.get(baseDir.getAbsolutePath().toString() + File.separatorChar + "d" + File.separatorChar + "ef");

    //Files - target/${uuid}/a/file.txt, target/${uuid}/d/file.txt
    Path filePath1 = Paths.get(dirPath1.getParent().toString() + File.separatorChar + "file.txt");
    Path filePath2 = Paths.get(dirPath2.getParent().toString() + File.separatorChar + "file.txt");

    Files.createDirectories(dirPath1);
    Files.createDirectories(dirPath2);

    Files.createFile(filePath1);
    Files.createFile(filePath2);
    FileFinder ff = new SynchronousFileFinder(
        Paths.get(baseDir.getAbsolutePath(), "*/*"),
        FileFilterOption.FILTER_DIRECTORY_REGULAR_FILES
    );
    Set<Path> paths = ff.find();
    Assert.assertEquals(4L, paths.size());
    Assert.assertTrue(paths.containsAll(Arrays.asList(dirPath1, dirPath2, filePath1, filePath2)));
  }

}
