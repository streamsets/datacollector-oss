/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.config.FileRollMode;
import com.streamsets.pipeline.config.PostProcessingOptions;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class TestGlobFileContextProvider {

  @Test
  public void testProvider() throws Exception {
    File testDir1 = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir1.mkdirs());
    File testDir2 = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir2.mkdirs());
    File file1 = new File(testDir1, "f1.txt");
    File file2 = new File(testDir2, "f2.txt");
    File file3 = new File(testDir1, "f3.txt");
    MultiFileInfo di1 = new MultiFileInfo("tag1", file1.getPath(), FileRollMode.REVERSE_COUNTER, "", "");
    MultiFileInfo di2 = new MultiFileInfo("tag2", file2.getPath(), FileRollMode.REVERSE_COUNTER, "", "");
    MultiFileInfo di3 = new MultiFileInfo("tag3", file3.getPath(), FileRollMode.REVERSE_COUNTER, "", "");

    FileEventPublisher eventPublisher = new FileEventPublisher() {
      @Override
      public void publish(FileEvent event) {
      }
    };

    GlobFileContextProvider provider =
        new GlobFileContextProvider(Arrays.asList(di1, di2, di3), 1, StandardCharsets.UTF_8, 1024,
                                    PostProcessingOptions.NONE, null, eventPublisher);


    // do full loop with no files
    provider.setOffsets(new HashMap<String, String>());
    Assert.assertTrue(provider.didFullLoop());
    provider.getOffsets();


    // do full loop with one file
    Files.createFile(file1.toPath());
    Thread.sleep(2000);
    provider.setOffsets(new HashMap<String, String>());
    Assert.assertFalse(provider.didFullLoop());
    Assert.assertEquals(di1, provider.next().getMultiFileInfo().getSource());
    Assert.assertTrue(provider.didFullLoop());
    provider.getOffsets();

    // do full loop with 3 files
    Files.createFile(file2.toPath());
    Files.createFile(file3.toPath());
    Thread.sleep(2000);
    Set<MultiFileInfo> expected = ImmutableSet.of(di1, di2, di3);
    Set<MultiFileInfo> got = new HashSet<>();
    provider.setOffsets(new HashMap<String, String>());
    Assert.assertFalse(provider.didFullLoop());
    got.add(provider.next().getMultiFileInfo().getSource());
    Assert.assertFalse(provider.didFullLoop());
    got.add(provider.next().getMultiFileInfo().getSource());
    Assert.assertFalse(provider.didFullLoop());
    got.add(provider.next().getMultiFileInfo().getSource());
    Assert.assertTrue(provider.didFullLoop());
    Assert.assertEquals(expected, got);

    // test reset loop count
    provider.startNewLoop();
    Assert.assertFalse(provider.didFullLoop());
    provider.getOffsets();

    // do partial loop
    provider.setOffsets(new HashMap<String, String>());
    Assert.assertFalse(provider.didFullLoop());
    provider.next().getMultiFileInfo().getSource();
    Assert.assertFalse(provider.didFullLoop());
    provider.getOffsets();

    // do full loop continuing from partial loop
    provider.setOffsets(new HashMap<String, String>());
    Assert.assertFalse(provider.didFullLoop());
    provider.next().getMultiFileInfo().getSource();
    Assert.assertFalse(provider.didFullLoop());
    provider.next().getMultiFileInfo().getSource();
    Assert.assertFalse(provider.didFullLoop());
    provider.next().getMultiFileInfo().getSource();
    Assert.assertTrue(provider.didFullLoop());
    provider.getOffsets();

    Files.delete(file2.toPath());
    Files.delete(testDir2.toPath());

    // do full loop now with 2 files
    provider.setOffsets(new HashMap<String, String>());
    Assert.assertFalse(provider.didFullLoop());
    provider.next().getMultiFileInfo().getSource();
    Assert.assertFalse(provider.didFullLoop());
    provider.next().getMultiFileInfo().getSource();
    Assert.assertTrue(provider.didFullLoop());
    provider.getOffsets();

    provider.close();

  }
}
