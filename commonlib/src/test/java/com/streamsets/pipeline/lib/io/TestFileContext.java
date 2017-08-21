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

import com.streamsets.pipeline.config.FileRollMode;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.sun.management.UnixOperatingSystemMXBean;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.UUID;

public class TestFileContext {

  private long getOpenFileDescriptors() {
    OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
    if (os instanceof UnixOperatingSystemMXBean) {
      return ((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount();
    } else {
      return -1;
    }
  }

  @Test(expected = IOException.class)
  public void testBeyondEOF() throws Exception {
    File testDir1 = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir1.mkdirs());
    File file1 = new File(testDir1, "f1.txt");
    Files.write(file1.toPath(), Arrays.asList("a", "b", "c"), StandardCharsets.UTF_8);
    MultiFileInfo di1 = new MultiFileInfo("tag1", file1.getPath(), FileRollMode.REVERSE_COUNTER, "", "", "");

    FileEventPublisher publisher = new FileEventPublisher() {
      @Override
      public void publish(FileEvent event) {
      }
    };

    long openFiles = getOpenFileDescriptors();
    try {
      FileContext context =
          new FileContext(di1, StandardCharsets.UTF_8, 100, PostProcessingOptions.NONE, null, publisher, false);
      context.setStartingCurrentFileName(new LiveFile(file1.toPath()));
      context.setStartingOffset(20);

      context.getReader();
    } finally {
      Assert.assertEquals(openFiles, getOpenFileDescriptors());
    }
  }

}
