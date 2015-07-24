/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
          new FileContext(di1, StandardCharsets.UTF_8, 100, PostProcessingOptions.NONE, null, publisher);
      context.setStartingCurrentFileName(new LiveFile(file1.toPath()));
      context.setStartingOffset(20);

      context.getReader();
    } finally {
      Assert.assertEquals(openFiles, getOpenFileDescriptors());
    }
  }

}
