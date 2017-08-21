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
    MultiFileInfo di1 = new MultiFileInfo("tag1", file1.getPath(), FileRollMode.REVERSE_COUNTER, "", "", "");
    MultiFileInfo di2 = new MultiFileInfo("tag2", file2.getPath(), FileRollMode.REVERSE_COUNTER, "", "", "");
    MultiFileInfo di3 = new MultiFileInfo("tag3", file3.getPath(), FileRollMode.REVERSE_COUNTER, "", "", "");

    FileEventPublisher eventPublisher = new FileEventPublisher() {
      @Override
      public void publish(FileEvent event) {
      }
    };

    ExactFileContextProvider provider = new ExactFileContextProvider(
        Arrays.asList(di1, di2, di3),
        StandardCharsets.UTF_8,
        1024,
        PostProcessingOptions.NONE,
        null,
        eventPublisher,
        false
    );


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
