/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.hdfs;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestHdfsDirectorySpooler {
  private List<String> dirPathList;
  private List<String> fileNameList;
  private String TEST_FOLDER_ROOT;
  private final String TARGET = "target";

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder(new File("./target"));

  @Before
  public void setUp() throws Exception {
    dirPathList = ImmutableList.of("2018-01-01", "2018-01-02", "2018-01-03", "2018-01-04", "test");
    fileNameList = ImmutableList.of("1.txt", "2.txt", "3.txt");
    for (String dirPath : dirPathList) {
      testFolder.newFolder(dirPath);
      for (String fileName : fileNameList) {
        testFolder.newFile(String.format("%s/%s", dirPath, fileName));
      }
    }
    TEST_FOLDER_ROOT = testFolder.getRoot().getAbsolutePath().replace(String.format("./%s", TARGET), TARGET);
  }

  @SuppressWarnings("unchecked")
  private PushSource.Context getContext() {
    return (PushSource.Context) ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  @Test
  public void testSpooler() throws Exception {
    HdfsDirectorySpooler spooler = null;

    try {
      FileSystem fs = FileSystem.newInstance(new URI("file:///"), new Configuration());

      spooler = HdfsDirectorySpooler.builder()
          .setContext(getContext())
          .setDir(String.format("%s/%s", testFolder.getRoot().getAbsolutePath(), "*-*-*"))
          .setFilePattern("*")
          .setFileSystem(fs)
          .setFirstFile("")
          .setMaxSpoolFiles(100)
          .build();

      spooler.init();

      Assert.assertTrue(spooler.isRunning());

      for (String dirPath : dirPathList) {
        if (dirPath.equals("test")) {
          continue;
        }
        for (String fileName : fileNameList) {
          FileStatus fileStatus = spooler.poolForFile(1, TimeUnit.SECONDS);
          Assert.assertTrue(String.format("%s/%s", dirPath, fileName), fileStatus != null);
        }
      }

      FileStatus fileStatus = spooler.poolForFile(1, TimeUnit.SECONDS);
      Assert.assertTrue(fileStatus == null);

    } finally {
      if (spooler != null) {
        spooler.destroy();
        Assert.assertFalse(spooler.isRunning());
      }
    }
  }

  @Test
  public void testSpoolerWithFirstFile() throws Exception {
    HdfsDirectorySpooler spooler = null;

    try {
      FileSystem fs = FileSystem.newInstance(new URI("file:///"), new Configuration());

      final String firstFile = TEST_FOLDER_ROOT + "/2018-01-04/1.txt";
      spooler = HdfsDirectorySpooler.builder()
          .setContext(getContext())
          .setDir(String.format("%s/%s", testFolder.getRoot().getAbsolutePath(), "*-*-*"))
          .setFilePattern("*")
          .setFileSystem(fs)
          .setFirstFile(firstFile)
          .setMaxSpoolFiles(100)
          .build();

      spooler.init();

      Assert.assertTrue(spooler.isRunning());

      FileStatus fileStatus = spooler.poolForFile(1, TimeUnit.SECONDS);
      int count = 0;

      while (fileStatus != null) {
        fileStatus = spooler.poolForFile(1, TimeUnit.SECONDS);
        count++;
      }

      Assert.assertEquals(3, count);
    } finally {
      if (spooler != null) {
        spooler.destroy();
        Assert.assertFalse(spooler.isRunning());
      }
    }
  }
}
