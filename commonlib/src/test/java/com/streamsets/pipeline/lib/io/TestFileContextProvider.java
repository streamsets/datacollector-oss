/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import com.streamsets.pipeline.config.FileRollMode;
import com.streamsets.pipeline.config.PostProcessingOptions;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;

public class TestFileContextProvider {

  @Test
  public void testProvider() throws Exception {
    File testDir1 = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir1.mkdirs());
    File file1 = new File(testDir1, "f1.txt");
    File file2 = new File(testDir1, "f2.txt");
    File file3 = new File(testDir1, "f3.txt");
    MultiFileInfo di1 = new MultiFileInfo("tag1", file1.getPath(), FileRollMode.REVERSE_COUNTER, "", "");
    MultiFileInfo di2 = new MultiFileInfo("tag2", file2.getPath(), FileRollMode.REVERSE_COUNTER, "", "");
    MultiFileInfo di3 = new MultiFileInfo("tag3", file3.getPath(), FileRollMode.REVERSE_COUNTER, "", "");

    FileEventPublisher eventPublisher = new FileEventPublisher() {
      @Override
      public void publish(FileEvent event) {
      }
    };

    FileContextProvider provider = new FileContextProvider(Arrays.asList(di1, di2, di3), StandardCharsets.UTF_8, 1024,
                                                           PostProcessingOptions.NONE, null, eventPublisher);


    // do full loop
    provider.setOffsets(new HashMap<String, String>());
    Assert.assertFalse(provider.didFullLoop());
    Assert.assertEquals(di1, provider.next().getMultiFileInfo());
    Assert.assertFalse(provider.didFullLoop());
    Assert.assertEquals(di2, provider.next().getMultiFileInfo());
    Assert.assertFalse(provider.didFullLoop());
    Assert.assertEquals(di3, provider.next().getMultiFileInfo());
    Assert.assertTrue(provider.didFullLoop());
    // test reset loop count
    provider.startNewLoop();
    Assert.assertFalse(provider.didFullLoop());
    provider.getOffsets();


    // do partial loop
    provider.setOffsets(new HashMap<String, String>());
    Assert.assertFalse(provider.didFullLoop());
    Assert.assertEquals(di1, provider.next().getMultiFileInfo());
    provider.getOffsets();

    // do full loop continuing from partial loop
    provider.setOffsets(new HashMap<String, String>());
    Assert.assertFalse(provider.didFullLoop());
    Assert.assertEquals(di2, provider.next().getMultiFileInfo());
    Assert.assertFalse(provider.didFullLoop());
    Assert.assertEquals(di3, provider.next().getMultiFileInfo());
    Assert.assertFalse(provider.didFullLoop());
    Assert.assertEquals(di1, provider.next().getMultiFileInfo());
    Assert.assertTrue(provider.didFullLoop());
    provider.getOffsets();

    provider.close();

  }
}
