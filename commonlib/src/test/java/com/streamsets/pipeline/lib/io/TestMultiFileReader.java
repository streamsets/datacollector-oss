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
import com.streamsets.pipeline.sdk.DataCollectorServicesUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

public class TestMultiFileReader {
  private static final Charset UTF8 = StandardCharsets.UTF_8;
  private File testDir1;
  private File testDir2;

  @BeforeClass
  public static void setUpClass() {
    DataCollectorServicesUtils.loadDefaultServices();
  }

  @Before
  public void setUp() {
    testDir1 = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir1.mkdirs());
    testDir2 = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir2.mkdirs());
  }

  @Test
  public void testEmptyDirectory() throws IOException {
    File file = new File(testDir1, "file.txt");
    MultiFileInfo di =
        new MultiFileInfo(null, file.getPath(), FileRollMode.REVERSE_COUNTER, "", "", "");
    MultiFileReader mdr = new MultiFileReader(Arrays.asList(di),
        UTF8,
        1024,
        PostProcessingOptions.NONE,
        null,
        false,
        0,
        false,
        false
    );
    mdr.setOffsets(new HashMap<String, String>());
    long start = System.currentTimeMillis();

    Assert.assertNull(mdr.next(20));
    Assert.assertTrue(System.currentTimeMillis() - start >= 20);
    Assert.assertEquals(1, mdr.getOffsets().size());
    Assert.assertNotNull("", mdr.getOffsets().get(di.getFileKey()));
    mdr.close();
  }

  @Test
  public void testWithOneDirectory() throws IOException {
    File file = new File(testDir1, "file.txt");
    Files.write(file.toPath(), Arrays.asList("Hello"), UTF8);
    MultiFileInfo di =
        new MultiFileInfo("tag", file.getPath(), FileRollMode.REVERSE_COUNTER, "", "", "");
    MultiFileReader mdr = new MultiFileReader(Arrays.asList(di),
        UTF8,
        1024,
        PostProcessingOptions.NONE,
        null,
        false,
        0,
        false,
        false
    );
    mdr.setOffsets(new HashMap<String, String>());
    long start = System.currentTimeMillis();
    LiveFileChunk chunk = mdr.next(1000);

    Assert.assertTrue(System.currentTimeMillis() - start < 1000);
    Assert.assertNotNull(chunk);
    Assert.assertEquals("tag", chunk.getTag());
    Assert.assertEquals("Hello\n", chunk.getLines().get(0).getText());
    Assert.assertEquals(1, mdr.getOffsets().size());
    Assert.assertTrue(mdr.getOffsets().get(di.getFileKey()).startsWith("6"));
    Assert.assertTrue(mdr.getOffsets().get(di.getFileKey()).contains("file.txt"));
    Assert.assertNull(mdr.next(0));

    Files.write(new File(testDir1, "file.txt").toPath(), Arrays.asList("Bye"), UTF8, StandardOpenOption.APPEND);
    mdr.setOffsets(mdr.getOffsets());
    chunk = mdr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertEquals("Bye\n", chunk.getLines().get(0).getText());
    Assert.assertEquals(1, mdr.getOffsets().size());
    Assert.assertTrue(mdr.getOffsets().get(di.getFileKey()).startsWith("10"));
    Assert.assertTrue(mdr.getOffsets().get(di.getFileKey()).contains("file.txt"));
    Assert.assertNull(mdr.next(0));

    mdr.close();
  }

  @Test(expected = IOException.class)
  public void testWithMultipleFilesInSameDirectoryWithSameName() throws Exception {
    File file1 = new File(testDir1, "f1.txt");
    Files.write(file1.toPath(), Arrays.asList("f1.0"), UTF8);
    MultiFileInfo di1 =
        new MultiFileInfo(null, file1.getPath(), FileRollMode.REVERSE_COUNTER, "", "", "");
    MultiFileInfo di2 =
        new MultiFileInfo(null, file1.getPath(), FileRollMode.REVERSE_COUNTER, "", "", "");
    new MultiFileReader(Arrays.asList(di1, di2), UTF8, 1024, PostProcessingOptions.NONE, null, false, 0, false, false);
  }

  @Test
  public void testWithMultipleDirectories() throws Exception {
    File file1 = new File(testDir1, "f1.txt");
    File file2 = new File(testDir1, "f2.txt");
    Files.write(file1.toPath(), Arrays.asList("f1.0"), UTF8);
    Files.write(file2.toPath(), Arrays.asList("f2.00"), UTF8);
    MultiFileInfo di1 =
        new MultiFileInfo("tag1", file1.getPath(), FileRollMode.REVERSE_COUNTER, "", "", "");
    MultiFileInfo di2 =
        new MultiFileInfo("tag2", file2.getPath(), FileRollMode.REVERSE_COUNTER, "", "", "");
    MultiFileReader mdr = new MultiFileReader(Arrays.asList(di1, di2),
        UTF8,
        1024,
        PostProcessingOptions.NONE,
        null,
        false,
        0,
        false,
        false
    );


    // just open the multidir, no file events
    Assert.assertTrue(mdr.getEvents().isEmpty());

    // reads first dir
    mdr.setOffsets(new HashMap<String, String>());

    // after setOffset there should be no file events
    Assert.assertTrue(mdr.getEvents().isEmpty());

    LiveFileChunk chunk = mdr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertEquals("tag1", chunk.getTag());
    Assert.assertEquals("f1.0\n", chunk.getLines().get(0).getText());
    Assert.assertEquals(2, mdr.getOffsets().size());
    Assert.assertTrue(mdr.getOffsets().get(di1.getFileKey()).startsWith("5"));
    Assert.assertTrue(mdr.getOffsets().get(di1.getFileKey()).contains("f1.txt"));
    Assert.assertTrue(mdr.getOffsets().get(di2.getFileKey()).isEmpty());

    //after first read we should get 1st file start event
    Assert.assertEquals(1, mdr.getEvents().size());
    LiveFile lf1 = new LiveFile(file1.toPath());
    Assert.assertEquals(new FileEvent(lf1, FileEvent.Action.START), mdr.getEvents().get(0));

    Files.write(file1.toPath(), Arrays.asList("f1.01"), UTF8, StandardOpenOption.APPEND);

    // reads second dir even if first dir has new data (round robin to avoid starvation)
    mdr.setOffsets(mdr.getOffsets());

    // after setOffset there should be no file events
    Assert.assertTrue(mdr.getEvents().isEmpty());

    chunk = mdr.next(0);

    //after first read we should get 2nd file start event
    Assert.assertEquals(1, mdr.getEvents().size());
    LiveFile lf2 = new LiveFile(file2.toPath());
    Assert.assertEquals(new FileEvent(lf2, FileEvent.Action.START), mdr.getEvents().get(0));

    Assert.assertNotNull(chunk);
    Assert.assertEquals("tag2", chunk.getTag());
    Assert.assertEquals("f2.00\n", chunk.getLines().get(0).getText());
    Assert.assertEquals(2, mdr.getOffsets().size());
    Assert.assertTrue(mdr.getOffsets().get(di1.getFileKey()).startsWith("5"));
    Assert.assertTrue(mdr.getOffsets().get(di1.getFileKey()).contains("f1.txt"));
    Assert.assertTrue(mdr.getOffsets().get(di2.getFileKey()).startsWith("6"));
    Assert.assertTrue(mdr.getOffsets().get(di2.getFileKey()).contains("f2.txt"));

    // reads first dir cause has data
    mdr.setOffsets(mdr.getOffsets());
    chunk = mdr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertEquals("tag1", chunk.getTag());
    Assert.assertEquals("f1.01\n", chunk.getLines().get(0).getText());

    // no data in any dir
    mdr.setOffsets(mdr.getOffsets());
    chunk = mdr.next(0);

    // no file events, we keep reading the same files
    Assert.assertTrue(mdr.getEvents().isEmpty());

    Assert.assertNull(chunk);

    Files.write(file2.toPath(), Arrays.asList("f2.01"), UTF8, StandardOpenOption.APPEND);

    // reads any dir with data
    mdr.setOffsets(mdr.getOffsets());
    chunk = mdr.next(0);

    // no file events, we keep reading the same files
    Assert.assertTrue(mdr.getEvents().isEmpty());

    Assert.assertNotNull(chunk);
    Assert.assertEquals("tag2", chunk.getTag());
    Assert.assertEquals("f2.01\n", chunk.getLines().get(0).getText());
    Assert.assertEquals(2, mdr.getOffsets().size());

    Files.write(file2.toPath(), Arrays.asList("f2.02"), UTF8, StandardOpenOption.APPEND);

    Files.move(file2.toPath(), Paths.get(file2 + ".1"));

    //lets sleep a bit more than the refresh interval in order to detect the rename
    Thread.sleep(SingleLineLiveFileReader.REFRESH_INTERVAL + 1);

    // reads rolled file from second dir
    mdr.setOffsets(mdr.getOffsets());
    chunk = mdr.next(0);

    //after first read we should get 1 file end event for the original lf2
    LiveFile oldLf2 = lf2.refresh(); //old because it is renamed
    Assert.assertEquals(1, mdr.getEvents().size());
    Assert.assertEquals(new FileEvent(oldLf2, FileEvent.Action.END), mdr.getEvents().get(0));

    Assert.assertNotNull(chunk);
    Assert.assertEquals("tag2", chunk.getTag());
    Assert.assertEquals("f2.02\n", chunk.getLines().get(0).getText());
    Assert.assertEquals(2, mdr.getOffsets().size());

    Files.write(file2.toPath(), Arrays.asList("f2.03"), UTF8, StandardOpenOption.CREATE);

    // reads live file from second dir
    mdr.setOffsets(mdr.getOffsets());
    chunk = mdr.next(0);

    //after first read we should get 1 file start event for the new lf2
    Assert.assertEquals(1, mdr.getEvents().size());
    Assert.assertEquals(new FileEvent(new LiveFile(file2.toPath()), FileEvent.Action.START), mdr.getEvents().get(0));

    long start = System.currentTimeMillis();
    while (chunk == null && System.currentTimeMillis() - start < 10000) {
      // we need to do sleep for a bit to ensure data is flushed to the FS
      Thread.sleep(100);

      // reads live file from second dir
      mdr.setOffsets(mdr.getOffsets());
      chunk = mdr.next(0);
    }

    Assert.assertNotNull(chunk);
    Assert.assertEquals("tag2", chunk.getTag());
    Assert.assertFalse(chunk.getLines().isEmpty());
    Assert.assertEquals("f2.03\n", chunk.getLines().get(0).getText());
    Assert.assertEquals(2, mdr.getOffsets().size());

    Assert.assertNull(mdr.next(0));
    mdr.close();
  }

  // log roll mode has a different live file strategy, so verifying things work there too
  @Test
  public void testPostProcessingDeleteLogRollMode() throws Exception {
    File file = new File(testDir1, "f1.txt");
    File file1 = new File(testDir1, "f1.txt.1");
    Files.write(file1.toPath(), Arrays.asList("f1"), UTF8);
    MultiFileInfo di1 =
        new MultiFileInfo(null, file.getPath(), FileRollMode.REVERSE_COUNTER, "", "", "");
    MultiFileReader mdr = new MultiFileReader(Arrays.asList(di1),
        UTF8,
        1024,
        PostProcessingOptions.DELETE,
        null,
        false,
        0,
        false,
        false
    );

    Assert.assertTrue(file1.exists());
    mdr.setOffsets(new HashMap<String, String>());

    //read file content
    Assert.assertNotNull(mdr.next(0));

    //reach eof
    Assert.assertNull(mdr.next(0));

    Assert.assertFalse(file1.exists());
    mdr.close();
  }

  @Test
  public void testPostProcessingDelete() throws Exception {
    File file1 = new File(testDir1, "f1.txt");
    File file2 = new File(testDir1, "f2.txt");
    Files.write(file1.toPath(), Arrays.asList("f1"), UTF8);
    MultiFileInfo di1 =
        new MultiFileInfo(null, new File(testDir1, "f${PATTERN}.txt").getPath(),
            FileRollMode.PATTERN, ".", "", "");

    MultiFileReader mdr = new MultiFileReader(Arrays.asList(di1),
        UTF8,
        1024,
        PostProcessingOptions.DELETE,
        null,
        false,
        0,
        false,
        false
    );

    Assert.assertTrue(file1.exists());
    mdr.setOffsets(new HashMap<String, String>());

    //read file content
    Assert.assertNotNull(mdr.next(0));

    //triggers a periodic 'roll'
    Files.createFile(file2.toPath());

    //sleeps to trigger a livefile refresh
    Thread.sleep(SingleLineLiveFileReader.REFRESH_INTERVAL * 2 + 1);

    //reach eof
    Assert.assertNull(mdr.next(0));

    Assert.assertFalse(file1.exists());
    mdr.close();
  }

  @Test
  public void testPostProcessingArchive() throws Exception {
    File file1 = new File(testDir1, "f1.txt");
    File file2 = new File(testDir1, "f2.txt");
    Files.write(file1.toPath(), Arrays.asList("f1"), UTF8);
    MultiFileInfo di1 =
        new MultiFileInfo(null, new File(testDir1, "f${PATTERN}.txt").getPath(),
            FileRollMode.PATTERN, ".", "", "");

    MultiFileReader mdr = new MultiFileReader(Arrays.asList(di1),
        UTF8,
        1024,
        PostProcessingOptions.ARCHIVE,
        testDir2.getAbsolutePath(),
        false,
        0,
        false,
        false
    );

    Assert.assertTrue(file1.exists());
    mdr.setOffsets(new HashMap<String, String>());

    //read file content
    Assert.assertNotNull(mdr.next(0));

    //triggers a periodic 'roll'
    Files.createFile(file2.toPath());

    //sleeps to trigger a livefile refresh
    Thread.sleep(SingleLineLiveFileReader.REFRESH_INTERVAL + 1);

    //reach eof
    Assert.assertNull(mdr.next(0));

    Assert.assertFalse(file1.exists());
    Path f1Archived = Paths.get(testDir2.getAbsolutePath(), file1.getPath());
    Assert.assertTrue(Files.exists(f1Archived));
    mdr.close();
  }

  @Test
  public void testPostProcessingDeleteInPreviewMode() throws Exception {
    File file1 = new File(testDir1, "f1.txt");
    File file2 = new File(testDir1, "f2.txt");
    Files.write(file1.toPath(), Arrays.asList("f1"), UTF8);
    MultiFileInfo di1 =
        new MultiFileInfo(null, new File(testDir1, "f${PATTERN}.txt").getPath(),
            FileRollMode.PATTERN, ".", "", "");

    MultiFileReader mdr = new MultiFileReader(Arrays.asList(di1),
        UTF8,
        1024,
        PostProcessingOptions.DELETE,
        null,
        false,
        0,
        false,
        true
    );

    Assert.assertTrue(file1.exists());
    mdr.setOffsets(new HashMap<String, String>());

    //read file content
    Assert.assertNotNull(mdr.next(0));

    //triggers a periodic 'roll'
    Files.createFile(file2.toPath());

    //sleeps to trigger a livefile refresh
    Thread.sleep(SingleLineLiveFileReader.REFRESH_INTERVAL * 2 + 1);

    //reach eof
    Assert.assertNull(mdr.next(0));

    Assert.assertTrue(file1.exists());
    mdr.close();
  }

  @Test
  public void testPostProcessingArchiveInPreviewMode() throws Exception {
    File file1 = new File(testDir1, "f1.txt");
    File file2 = new File(testDir1, "f2.txt");
    Files.write(file1.toPath(), Arrays.asList("f1"), UTF8);
    MultiFileInfo di1 =
        new MultiFileInfo(null, new File(testDir1, "f${PATTERN}.txt").getPath(),
            FileRollMode.PATTERN, ".", "", "");

    MultiFileReader mdr = new MultiFileReader(Arrays.asList(di1),
        UTF8,
        1024,
        PostProcessingOptions.ARCHIVE,
        testDir2.getAbsolutePath(),
        false,
        0,
        false,
        true
    );

    Assert.assertTrue(file1.exists());
    mdr.setOffsets(new HashMap<String, String>());

    //read file content
    Assert.assertNotNull(mdr.next(0));

    //triggers a periodic 'roll'
    Files.createFile(file2.toPath());

    //sleeps to trigger a livefile refresh
    Thread.sleep(SingleLineLiveFileReader.REFRESH_INTERVAL + 1);

    //reach eof
    Assert.assertNull(mdr.next(0));

    Assert.assertTrue(file1.exists());
    Path f1Archived = Paths.get(testDir2.getAbsolutePath(), file1.getPath());
    Assert.assertFalse(Files.exists(f1Archived));
    mdr.close();
  }

  @Test
  public void testMultiLineFiles() throws Exception {
    File file1 = new File(testDir1, "f1.txt");
    File file2 = new File(testDir1, "f2.txt");
    Files.write(file1.toPath(), Arrays.asList("A1M1", "a1m1", "A1M2"), UTF8);
    Files.write(file2.toPath(), Arrays.asList("B2M1", "B2M2", "b2m2", "B3M3"), UTF8);
    MultiFileInfo di1 = new MultiFileInfo("tag1", file1.getPath(), FileRollMode.REVERSE_COUNTER, "", "", "^A.*");
    MultiFileInfo di2 = new MultiFileInfo("tag2", file2.getPath(), FileRollMode.REVERSE_COUNTER, "", "", "^B.*");
    MultiFileReader mdr = new MultiFileReader(Arrays.asList(di1, di2),
        UTF8,
        1024,
        PostProcessingOptions.NONE,
        null,
        false,
        0,
        false,
        false
    );

    Set<String> lines = new HashSet<>();
    Map<String, String> offsets = new HashMap<>();
    while (lines.size() < 3) {
      mdr.setOffsets(offsets);
      LiveFileChunk chunk = mdr.next(0);
      if (chunk != null) {
        for (FileLine line : chunk.getLines()) {
          lines.add(line.getText());
        }
      }
      offsets = mdr.getOffsets();
    }
    mdr.close();

  }

  private String getRandomData(int numOfLines, int lineLength) {
    final Random r = new Random();
    StringBuilder sb = new StringBuilder();
    for (int i =0 ;i < numOfLines; i++) {
      for (int j=0; j < lineLength; j++) {
        sb.append((char)(97 + r.nextInt(25)));
      }
      sb.append("\n");
    }
    return sb.toString();
  }

  @Test(timeout = 2000)
  public void testArchivingWithOffsetLagAndPendingFiles() throws Exception {
    File testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    File archiveDir = new File(testDir, "archive").getAbsoluteFile();

    int totalNumOfFiles = 8, numOfFilesPerDir = 2, numOfLinesPerFile = 20;

    Assert.assertTrue(testDir.mkdirs());
    Assert.assertTrue(archiveDir.mkdirs());

    String data = getRandomData(numOfLinesPerFile, 50);
    File innerDir = null;
    Set<String> dirs = new LinkedHashSet<>();

    int numOfFileForCurrentDir = 0;
    for (int i = 0; i < totalNumOfFiles; i++) {
      if (innerDir == null || numOfFileForCurrentDir == numOfFilesPerDir) {
        innerDir = new File(testDir, String.valueOf(i));
        Assert.assertTrue(innerDir.mkdirs());
        dirs.add(innerDir.getAbsolutePath());
        numOfFileForCurrentDir = 0;
      }
      String fileName = "file_" + i;
      Files.write(Paths.get(innerDir.getAbsolutePath() + "/" + fileName), data.getBytes());
      numOfFileForCurrentDir++;
    }
    List<MultiFileInfo> multiFileInfos = new ArrayList<>();
    int i = 0;
    for (String dir : dirs) {
      MultiFileInfo multiFileInfo =
          new MultiFileInfo("tag" + i, dir + "/${PATTERN}", FileRollMode.PATTERN, ".*", "", "");
      multiFileInfos.add(multiFileInfo);
      i++;
    }

    MultiFileReader mdr = new MultiFileReader(multiFileInfos,
        UTF8,
        1024,
        PostProcessingOptions.DELETE,
        archiveDir.getAbsolutePath(),
        true,
        1,
        false,
        false
    );

    Map<String, String> offsets = new HashMap<>();
    int numOfLinesProcessed = 0, totalNumberOfLines = totalNumOfFiles * numOfLinesPerFile;

    do {
      mdr.setOffsets(offsets);
      LiveFileChunk chunk = mdr.next(0);
      if (chunk != null) {
        numOfLinesProcessed += chunk.getLines().size();
      }
      offsets = mdr.getOffsets();
      mdr.getOffsetsLag(offsets);
      mdr.getPendingFiles();
    } while (numOfLinesProcessed < totalNumberOfLines);
  }
}
