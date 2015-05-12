/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import com.streamsets.pipeline.config.PostProcessingOptions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;

public class TestMultiDirectoryReader {
  private static final Charset UTF8 = Charset.forName("UTF-8");
  private File testDir1;
  private File testDir2;

  @Before
  public void setUp() {
    testDir1 = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir1.mkdirs());
    testDir2 = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir2.mkdirs());
  }

  @Test
  public void testEmptyDirectory() throws IOException {
    MultiDirectoryReader.DirectoryInfo di = new MultiDirectoryReader.DirectoryInfo(testDir1.getAbsolutePath(),
                                                                                   LogRollMode.REVERSE_COUNTER,
                                                                                   "file.txt", "");
    MultiDirectoryReader mdr = new MultiDirectoryReader(Arrays.asList(di), UTF8, 1024, PostProcessingOptions.NONE, null);
    mdr.setOffsets(new HashMap<String, String>());
    long start = System.currentTimeMillis();
    Assert.assertNull(mdr.next(20));
    Assert.assertTrue(System.currentTimeMillis() - start >= 20);
    Assert.assertEquals(1, mdr.getOffsets().size());
    Assert.assertNotNull("", mdr.getOffsets().get(testDir1.getAbsolutePath()));
    mdr.close();
  }

  @Test
  public void testWithOneDirectory() throws IOException {
    Files.write(new File(testDir1, "file.txt").toPath(), Arrays.asList("Hello"), UTF8);
    MultiDirectoryReader.DirectoryInfo di = new MultiDirectoryReader.DirectoryInfo(testDir1.getAbsolutePath(),
                                                                                   LogRollMode.REVERSE_COUNTER,
                                                                                   "file.txt", "");
    MultiDirectoryReader mdr = new MultiDirectoryReader(Arrays.asList(di), UTF8, 1024, PostProcessingOptions.NONE, null);
    mdr.setOffsets(new HashMap<String, String>());
    long start = System.currentTimeMillis();
    LiveFileChunk chunk = mdr.next(1000);
    Assert.assertTrue(System.currentTimeMillis() - start < 1000);
    Assert.assertNotNull(chunk);
    Assert.assertEquals("Hello\n", chunk.getLines().get(0).getText());
    Assert.assertEquals(1, mdr.getOffsets().size());
    Assert.assertTrue(mdr.getOffsets().get(testDir1.getAbsolutePath()).startsWith("6"));
    Assert.assertTrue(mdr.getOffsets().get(testDir1.getAbsolutePath()).contains("file.txt"));
    Assert.assertNull(mdr.next(0));

    Files.write(new File(testDir1, "file.txt").toPath(), Arrays.asList("Bye"), UTF8, StandardOpenOption.APPEND);
    mdr.setOffsets(mdr.getOffsets());
    chunk = mdr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertEquals("Bye\n", chunk.getLines().get(0).getText());
    Assert.assertEquals(1, mdr.getOffsets().size());
    Assert.assertTrue(mdr.getOffsets().get(testDir1.getAbsolutePath()).startsWith("10"));
    Assert.assertTrue(mdr.getOffsets().get(testDir1.getAbsolutePath()).contains("file.txt"));
    Assert.assertNull(mdr.next(0));

    mdr.close();
  }

  @Test(expected = IOException.class)
  public void testWithMultipleDirectoriesSameName() throws Exception {
    Files.write(new File(testDir1, "f1.txt").toPath(), Arrays.asList("f1.0"), UTF8);
    Files.write(new File(testDir1, "f2.txt").toPath(), Arrays.asList("f2.00"), UTF8);
    MultiDirectoryReader.DirectoryInfo di1 = new MultiDirectoryReader.DirectoryInfo(testDir1.getAbsolutePath(),
                                                                                    LogRollMode.REVERSE_COUNTER,
                                                                                    "f1.txt", "");
    MultiDirectoryReader.DirectoryInfo di2 = new MultiDirectoryReader.DirectoryInfo(testDir1.getAbsolutePath(),
                                                                                    LogRollMode.REVERSE_COUNTER,
                                                                                    "f2.txt", "");
    MultiDirectoryReader mdr = new MultiDirectoryReader(Arrays.asList(di1, di2), UTF8, 1024, PostProcessingOptions.NONE,
                                                        null);
  }

  @Test
  public void testWithMultipleDirectories() throws Exception {
    Path f1 = new File(testDir1, "f1.txt").toPath().toAbsolutePath();
    Path f2 = new File(testDir2, "f2.txt").toPath().toAbsolutePath();
    Files.write(f1, Arrays.asList("f1.0"), UTF8);
    Files.write(f2, Arrays.asList("f2.00"), UTF8);
    MultiDirectoryReader.DirectoryInfo di1 = new MultiDirectoryReader.DirectoryInfo(testDir1.getAbsolutePath(),
                                                                                    LogRollMode.REVERSE_COUNTER,
                                                                                    "f1.txt", "");
    MultiDirectoryReader.DirectoryInfo di2 = new MultiDirectoryReader.DirectoryInfo(testDir2.getAbsolutePath(),
                                                                                    LogRollMode.REVERSE_COUNTER,
                                                                                    "f2.txt", "");

    MultiDirectoryReader mdr = new MultiDirectoryReader(Arrays.asList(di1, di2), UTF8, 1024, PostProcessingOptions.NONE,
                                                        null);
    // just open the multidir, no file events
    Assert.assertTrue(mdr.getEvents().isEmpty());

    // reads first dir
    mdr.setOffsets(new HashMap<String, String>());

    // after setOffset there should be no file events
    Assert.assertTrue(mdr.getEvents().isEmpty());

    LiveFileChunk chunk = mdr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertEquals("f1.0\n", chunk.getLines().get(0).getText());
    Assert.assertEquals(2, mdr.getOffsets().size());
    Assert.assertTrue(mdr.getOffsets().get(testDir1.getAbsolutePath()).startsWith("5"));
    Assert.assertTrue(mdr.getOffsets().get(testDir1.getAbsolutePath()).contains("f1.txt"));
    Assert.assertTrue(mdr.getOffsets().get(testDir2.getAbsolutePath()).isEmpty());

    //after first read we should get 1st file start event
    Assert.assertEquals(1, mdr.getEvents().size());
    LiveFile lf1 = new LiveFile(f1);
    Assert.assertEquals(new MultiDirectoryReader.Event(lf1, true), mdr.getEvents().get(0));

    Files.write(new File(testDir1, "f1.txt").toPath(), Arrays.asList("f1.01"), UTF8, StandardOpenOption.APPEND);

    // reads second dir even if first dir has new data (round robin to avoid starvation)
    mdr.setOffsets(mdr.getOffsets());

    // after setOffset there should be no file events
    Assert.assertTrue(mdr.getEvents().isEmpty());

    chunk = mdr.next(0);

    //after first read we should get 2nd file start event
    Assert.assertEquals(1, mdr.getEvents().size());
    LiveFile lf2 = new LiveFile(f2);
    Assert.assertEquals(new MultiDirectoryReader.Event(lf2, true), mdr.getEvents().get(0));

    Assert.assertNotNull(chunk);
    Assert.assertEquals("f2.00\n", chunk.getLines().get(0).getText());
    Assert.assertEquals(2, mdr.getOffsets().size());
    Assert.assertTrue(mdr.getOffsets().get(testDir1.getAbsolutePath()).startsWith("5"));
    Assert.assertTrue(mdr.getOffsets().get(testDir1.getAbsolutePath()).contains("f1.txt"));
    Assert.assertTrue(mdr.getOffsets().get(testDir2.getAbsolutePath()).startsWith("6"));
    Assert.assertTrue(mdr.getOffsets().get(testDir2.getAbsolutePath()).contains("f2.txt"));

    // reads first dir cause has data
    mdr.setOffsets(mdr.getOffsets());
    chunk = mdr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertEquals("f1.01\n", chunk.getLines().get(0).getText());

    // no data in any dir
    mdr.setOffsets(mdr.getOffsets());
    chunk = mdr.next(0);

    // no file events, we keep reading the same files
    Assert.assertTrue(mdr.getEvents().isEmpty());

    Assert.assertNull(chunk);

    Files.write(new File(testDir2, "f2.txt").toPath(), Arrays.asList("f2.01"), UTF8, StandardOpenOption.APPEND);

    // reads any dir with data
    mdr.setOffsets(mdr.getOffsets());
    chunk = mdr.next(0);

    // no file events, we keep reading the same files
    Assert.assertTrue(mdr.getEvents().isEmpty());

    Assert.assertNotNull(chunk);
    Assert.assertEquals("f2.01\n", chunk.getLines().get(0).getText());
    Assert.assertEquals(2, mdr.getOffsets().size());

    Files.write(new File(testDir2, "f2.txt").toPath(), Arrays.asList("f2.02"), UTF8, StandardOpenOption.APPEND);

    Files.move(new File(testDir2, "f2.txt").toPath(), new File(testDir2, "f2.txt.1").toPath());

    //lets sleep a bit more than the refresh interval in order to detect the rename
    Thread.sleep(LiveFileReader.REFRESH_INTERVAL + 1);

    // reads rolled file from second dir
    mdr.setOffsets(mdr.getOffsets());
    chunk = mdr.next(0);

    //after first read we should get 1 file end event for the original lf2
    LiveFile oldLf2 = lf2.refresh(); //old because it is renamed
    Assert.assertEquals(1, mdr.getEvents().size());
    Assert.assertEquals(new MultiDirectoryReader.Event(oldLf2, false), mdr.getEvents().get(0));

    Assert.assertNotNull(chunk);
    Assert.assertEquals("f2.02\n", chunk.getLines().get(0).getText());
    Assert.assertEquals(2, mdr.getOffsets().size());

    Files.write(new File(testDir2, "f2.txt").toPath(), Arrays.asList("f2.03"), UTF8, StandardOpenOption.CREATE);

    // reads live file from second dir
    mdr.setOffsets(mdr.getOffsets());
    chunk = mdr.next(0);

    //after first read we should get 1 file start event for the new lf2
    Assert.assertEquals(1, mdr.getEvents().size());
    Assert.assertEquals(new MultiDirectoryReader.Event(new LiveFile(f2), true), mdr.getEvents().get(0));

    long start = System.currentTimeMillis();
    while (chunk == null && System.currentTimeMillis() - start < 10000) {
      // we need to do sleep for a bit to ensure data is flushed to the FS
      Thread.sleep(100);

      // reads live file from second dir
      mdr.setOffsets(mdr.getOffsets());
      chunk = mdr.next(0);
    }

    Assert.assertEquals("f2.03\n", chunk.getLines().get(0).getText());
    Assert.assertEquals(2, mdr.getOffsets().size());

    Assert.assertNull(mdr.next(0));
    mdr.close();
  }

  @Test
  public void testPostProcessingDelete() throws Exception {
    Path f1 = new File(testDir1, "f1.txt").toPath().toAbsolutePath();
    Path f2 = new File(testDir1, "f2.txt").toPath().toAbsolutePath();
    Files.write(f1, Arrays.asList("f1"), UTF8);
    PeriodicFilesRollMode rollMode = new PeriodicFilesRollMode();
    rollMode.setPattern("f.\\.txt");
    MultiDirectoryReader.DirectoryInfo di1 = new MultiDirectoryReader.DirectoryInfo(testDir1.getAbsolutePath(),
                                                                                    rollMode,  "", "");

    MultiDirectoryReader mdr = new MultiDirectoryReader(Arrays.asList(di1), UTF8, 1024, PostProcessingOptions.DELETE,
                                                        null);

    Assert.assertTrue(Files.exists(f1));
    mdr.setOffsets(new HashMap<String, String>());

    //read file content
    Assert.assertNotNull(mdr.next(0));

    //triggers a periodic 'roll'
    Files.createFile(f2);

    //sleeps to trigger a livefile refresh
    Thread.sleep(LiveFileReader.REFRESH_INTERVAL + 1);

    //reach eof
    Assert.assertNull(mdr.next(0));

    Assert.assertFalse(Files.exists(f1));
    mdr.close();
  }

  @Test
  public void testPostProcessingArchive() throws Exception {
    Path f1 = new File(testDir1, "f1.txt").toPath().toAbsolutePath();
    Path f2 = new File(testDir1, "f2.txt").toPath().toAbsolutePath();
    Files.write(f1, Arrays.asList("f1"), UTF8);
    PeriodicFilesRollMode rollMode = new PeriodicFilesRollMode();
    rollMode.setPattern("f.\\.txt");
    MultiDirectoryReader.DirectoryInfo di1 = new MultiDirectoryReader.DirectoryInfo(testDir1.getAbsolutePath(),
                                                                                    rollMode,  "", "");

    MultiDirectoryReader mdr = new MultiDirectoryReader(Arrays.asList(di1), UTF8, 1024, PostProcessingOptions.ARCHIVE,
                                                        testDir2.getAbsolutePath());

    Assert.assertTrue(Files.exists(f1));
    mdr.setOffsets(new HashMap<String, String>());

    //read file content
    Assert.assertNotNull(mdr.next(0));

    //triggers a periodic 'roll'
    Files.createFile(f2);

    //sleeps to trigger a livefile refresh
    Thread.sleep(LiveFileReader.REFRESH_INTERVAL + 1);

    //reach eof
    Assert.assertNull(mdr.next(0));

    Assert.assertFalse(Files.exists(f1));
    Path f1Archived = Paths.get(testDir2.getAbsolutePath(), f1.toString());
    Assert.assertTrue(Files.exists(f1Archived));
    mdr.close();
  }

}
