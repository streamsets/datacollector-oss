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
package com.streamsets.pipeline.stage.destination.hdfs.writer;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsDTarget;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsFileType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class TestRecordWriterManager {
  private static Path testDir;
  private static Target.Context targetContext = ContextInfoCreator.createTargetContext(HdfsDTarget.class,
    "testWritersLifecycle", false, OnRecordError.TO_ERROR, null);
  private static URI uri;
  private static Configuration hdfsConf = new HdfsConfiguration();

  @BeforeClass
  public static void initializeURI() {
    try {
      uri = new URI("file:///");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private RecordWriterManagerTestBuilder managerBuilder() throws URISyntaxException {
    return new RecordWriterManagerTestBuilder()
      .context(targetContext)
      .hdfsUri(uri)
      .hdfsConf(hdfsConf)
      .dirPathTemplate(getTestDir().toString())
      .generatorFactory(new DummyDataGeneratorFactory(null))
      .fileType(HdfsFileType.TEXT);
  }

  public static class DummyDataGeneratorFactory extends DataGeneratorFactory {
    protected DummyDataGeneratorFactory(Settings settings) {
      super(settings);
    }

    @Override
    public DataGenerator getGenerator(OutputStream os) throws IOException {
      return new DummyDataGenerator(os);
    }
  }

  public static class DummyDataGenerator implements DataGenerator {

    private final Writer writer;

    DummyDataGenerator (OutputStream os) {
      writer = new OutputStreamWriter(os);
    }

    @Override
    public void write(Record record) throws IOException, DataGeneratorException {
      writer.write(record.get().getValueAsString() + "\n");
    }

    @Override
    public void flush() throws IOException {
      writer.flush();
    }

    @Override
    public void close() throws IOException {
      writer.close();
    }
  }

  @BeforeClass
  public static void setUpClass() {
    File dir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(dir.mkdirs());
    testDir = new Path(dir.getAbsolutePath());
  }

  private Path getTestDir() {
    return testDir;
  }

  private Date parseDate(String str) throws Exception {
    DateFormat parser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    parser.setTimeZone(TimeZone.getTimeZone("UTC"));
    return parser.parse(str);
  }

  private Date getFixedDate() throws Exception {
    return parseDate("2015-01-20T09:56:01Z");
  }

  @Test
  public void testConstructorAndGetters() throws Exception {
    RecordWriterManager mgr = managerBuilder()
      .dirPathTemplate(getTestDir().toString() +
        "/${YYYY()}/${YY()}/${MM()}/${DD()}/${hh()}/${mm()}/${ss()}/" +
        "${not empty record:value('/') ? record:value('/') : 'blah'}")
      .cutOffSecs(10)
      .build();

    Date date = getFixedDate();

    Record record = RecordCreator.create();
    record.set(Field.create("a"));
    Assert.assertEquals(getTestDir() + "/2015/15/01/20/09/56/01/a", mgr.getDirPath(date, record));

    record.set(null);
    Assert.assertEquals(getTestDir() + "/2015/15/01/20/09/56/01/blah", mgr.getDirPath(date, record));

    Date now = date;
    Date recordDate = new Date(now.getTime() - 3000);
    Assert.assertEquals(7999, mgr.getTimeToLiveMillis(now, recordDate));
  }

  @Test
  public void testNoCompressionCodec() throws Exception {
    final String fileNameSuffix = "";
    testPath(null, fileNameSuffix);
  }

  @Test
  public void testCompressionCodec() throws Exception {
    final String fileNameSuffix = "";
    testPath(new DefaultCodec(), fileNameSuffix);
  }

  @Test
  public void testNameSuffix() throws Exception {
    final String fileNameSuffix = "txt";
    // test with and without compression codec
    testPath(null, fileNameSuffix);
    testPath(new DefaultCodec(), fileNameSuffix);
  }

  private void testPath(CompressionCodec compressionCodec, String fileNameSuffix) throws Exception {
    RecordWriterManager mgr = managerBuilder()
      .dirPathTemplate(getTestDir().toString() + "/${YYYY()}/${YY()}/${MM()}/${DD()}/${hh()}/${mm()}/${ss()}/${record:value('/')}")
      .fileType(HdfsFileType.TEXT)
      .compressionCodec(compressionCodec)
      .fileNameSuffix(fileNameSuffix)
      .build();

    if (compressionCodec == null && fileNameSuffix.isEmpty()) {
      Assert.assertEquals("", mgr.getExtension());
    } else if (compressionCodec != null && fileNameSuffix.isEmpty()) {
      Assert.assertEquals(compressionCodec.getDefaultExtension(), mgr.getExtension());
    } else if (compressionCodec == null && !fileNameSuffix.isEmpty()) {
      Assert.assertEquals("." + fileNameSuffix, mgr.getExtension());
    } else {
      Assert.assertEquals("." + fileNameSuffix + compressionCodec.getDefaultExtension(), mgr.getExtension());
    }

    Date date = getFixedDate();
    Record record = RecordCreator.create();
    record.set(Field.create("a"));

    String prefix = mgr.getPath(date, record).toString();
    Assert.assertTrue(
      Utils.format("Unexpected prefix: {}", prefix),
      prefix.startsWith(new Path(getTestDir(), "2015/15/01/20/09/56/01/a/_tmp_prefix_0").toString())
    );
  }

  private void testTextFile(CompressionCodec compressionCodec) throws Exception {
    RecordWriterManager mgr = managerBuilder()
      .dirPathTemplate(getTestDir().toString() + "/${YYYY()}")
      .fileType(HdfsFileType.TEXT)
      .compressionCodec(compressionCodec)
      .build();

    Assert.assertTrue(mgr.validateDirTemplate("g", "dirPathTemplate", "dirPathTemplate", new ArrayList<Stage.ConfigIssue>()));
    FileSystem fs = FileSystem.get(uri, hdfsConf);
    Path file = new Path(getTestDir(), UUID.randomUUID().toString());
    long expires = System.currentTimeMillis() + 50000;
    RecordWriter writer = mgr.createWriter(fs, file, 50000);
    Assert.assertTrue(expires <= writer.getExpiresOn());
    Assert.assertTrue(writer.isTextFile());
    Assert.assertFalse(writer.isSeqFile());
    Record record = RecordCreator.create();
    record.set(Field.create("a"));
    writer.write(record);
    writer.close();
    InputStream is = fs.open(file);
    if (compressionCodec != null) {
      is = compressionCodec.createInputStream(is);
    }
    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    Assert.assertEquals("a", reader.readLine());
    Assert.assertNull(reader.readLine());
    reader.close();
  }

  @Test
  public void testTextFileNoCompression() throws Exception {
    testTextFile(null);
  }

  @Test
  public void testTextFileCompression() throws Exception {
    DefaultCodec codec = new DefaultCodec();
    codec.setConf(new Configuration());
    testTextFile(codec);
  }

  private void testSeqFile(CompressionCodec compressionCodec, SequenceFile.CompressionType compressionType)
      throws Exception {
    RecordWriterManager mgr = managerBuilder()
      .dirPathTemplate(getTestDir().toString() + "/${YYYY()}")
      .compressionCodec(compressionCodec)
      .compressionType(compressionType)
      .fileType(HdfsFileType.SEQUENCE_FILE)
      .build();

    FileSystem fs = FileSystem.get(uri, hdfsConf);
    Path file = new Path(getTestDir(), UUID.randomUUID().toString());
    long expires = System.currentTimeMillis() + 50000;
    RecordWriter writer = mgr.createWriter(fs, file, 50000);
    Assert.assertTrue(expires <= writer.getExpiresOn());
    Assert.assertFalse(writer.isTextFile());
    Assert.assertTrue(writer.isSeqFile());
    Record record = RecordCreator.create();
    record.set(Field.create("a"));
    writer.write(record);
    writer.close();

    SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, new HdfsConfiguration());
    Text key = new Text();
    Text value = new Text();
    Assert.assertTrue(reader.next(key, value));
    Assert.assertNotNull(UUID.fromString(key.toString()));
    Assert.assertEquals("a", value.toString().trim());
    Assert.assertFalse(reader.next(key, value));
    reader.close();
  }

  @Test
  public void testSeqFileNoCompression() throws Exception {
    testSeqFile(null, SequenceFile.CompressionType.NONE);
  }

  @Test
  public void testSeqFileCompression() throws Exception {
    DefaultCodec codec = new DefaultCodec();
    codec.setConf(new Configuration());
    testSeqFile(codec, SequenceFile.CompressionType.RECORD);
    testSeqFile(codec, SequenceFile.CompressionType.BLOCK);
  }

  @Test
  public void testGetWriter() throws Exception {
    RecordWriterManager mgr = managerBuilder()
      .cutOffSecs(10)
      .cutOffSizeBytes(5)
      .cutOffRecords(2)
      .dirPathTemplate(getTestDir().toString() + "/${YYYY()}/${MM()}/${DD()}/${hh()}/${mm()}/${ss()}/${record:value('/')}")
      .build();

    FileSystem fs = FileSystem.get(uri, hdfsConf);
    Date now = getFixedDate();

    // record older than cut off
    Date recordDate = new Date(now.getTime() - 10 * 1000 - 1);
    Record record = RecordCreator.create();
    record.set(Field.create("a"));
    Assert.assertNull(mgr.getWriter(now, recordDate, record));

    // record qualifies, first file
    recordDate = new Date(now.getTime() - 10 * 1000 + 1);
    RecordWriter writer = mgr.getWriter(now, recordDate, record);
    Assert.assertNotNull(writer);
    Path tempPath = writer.getPath();
    Assert.assertEquals(mgr.getPath(recordDate, record), tempPath);
    Path finalPath = mgr.commitWriter(writer);
    //committing a closed writer is a NOP
    Assert.assertNull(mgr.commitWriter(writer));

    Assert.assertEquals(1, getFinalFileNameCount(fs, tempPath.getParent(), "prefix"));

    // record qualifies, second file
    writer = mgr.getWriter(now, recordDate, record);
    finalPath = mgr.commitWriter(writer);

    Assert.assertEquals(2, getFinalFileNameCount(fs, tempPath.getParent(), "prefix"));

    // record qualifies, leaving temp file
    writer = mgr.getWriter(now, recordDate, record);
    writer.close();

    // record qualifies, it should roll temp file and create 4th file
    writer = mgr.getWriter(now, recordDate, record);
    finalPath = mgr.commitWriter(writer);
    Assert.assertFalse(fs.exists(tempPath));
    Assert.assertEquals(4, getFinalFileNameCount(fs, tempPath.getParent(), "prefix"));

    // verifying thresholds because of record count
    writer = mgr.getWriter(now, recordDate, record);
    Assert.assertFalse(mgr.isOverThresholds(writer));
    writer.write(record);
    writer.flush();
    Assert.assertFalse(mgr.isOverThresholds(writer));
    writer.write(record);
    writer.flush();
    Assert.assertTrue(mgr.isOverThresholds(writer));
    writer.write(record);
    mgr.commitWriter(writer);

    // verifying thresholds because of file size
    writer = mgr.getWriter(now, recordDate, record);
    Assert.assertFalse(mgr.isOverThresholds(writer));
    record.set(Field.create("0123456789012345678901234567890123456789012345678901234567890123456789"));
    writer.write(record);
    writer.flush();
    Assert.assertTrue(mgr.isOverThresholds(writer));
    mgr.commitWriter(writer);
  }

  private int getFinalFileNameCount(FileSystem fs, Path dir, final String prefix) throws IOException {
    return fs.listStatus(dir, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith(prefix);
      }
    }).length;
  }

  @Test
  public void testMissingUniquePrefix() throws Exception {
    RecordWriterManager mgr = managerBuilder()
      .uniquePrefix("")
      .build();

    FileSystem fs = FileSystem.get(uri, hdfsConf);
    Record record = RecordCreator.create();
    record.set(Field.create("a"));


    RecordWriter writer = mgr.getWriter(new Date(), new Date(), record);
    Assert.assertNotNull(writer);

    Path finalPath = mgr.commitWriter(writer);

    Assert.assertNotNull(finalPath);
    Assert.assertTrue("Path should not start with '_'", !finalPath.toString().startsWith("_"));
  }

  @Test
  public void testThresholdRecords() throws Exception {
    RecordWriterManager mgr = managerBuilder()
      .build();

    Date now = getFixedDate();

    Date recordDate = now;
    Record record = RecordCreator.create();
    record.set(Field.create("a"));
    RecordWriter writer = mgr.getWriter(now, recordDate, record);
    Assert.assertNotNull(writer);
    for (int i = 0; i < 2; i++) {
      Assert.assertFalse(mgr.isOverThresholds(writer));
      writer.write(record);
      writer.flush();
    }
    Assert.assertTrue(mgr.isOverThresholds(writer));
    mgr.commitWriter(writer);
  }

  @Test
  public void testThresholdSize() throws Exception {
    RecordWriterManager mgr = managerBuilder()
      .cutOffSizeBytes(4)
      .build();

    Date now = getFixedDate();

    Date recordDate = now;
    Record record = RecordCreator.create();
    record.set(Field.create("a"));
    RecordWriter writer = mgr.getWriter(now, recordDate, record);
    Assert.assertNotNull(writer);
    for (int i = 0; i < 2; i++) {
      Assert.assertFalse(mgr.isOverThresholds(writer));
      writer.write(record);
      writer.flush();
    }
    Assert.assertTrue(mgr.isOverThresholds(writer));
    mgr.commitWriter(writer);
  }

  @Test
  public void testThresholdOverflow() throws Exception {
    RecordWriterManager mgr = managerBuilder()
      .cutOffSecs(Long.MAX_VALUE)
      .build();

    Date now = getFixedDate();

    Date recordDate = now;
    Record record = RecordCreator.create();
    record.set(Field.create("a"));
    RecordWriter writer = mgr.getWriter(now, recordDate, record);
    Assert.assertNotNull(writer);
    for (int i = 0; i < 2; i++) {
      Assert.assertFalse(mgr.isOverThresholds(writer));
      writer.write(record);
      writer.flush();
    }
    Assert.assertTrue(mgr.isOverThresholds(writer));
    mgr.commitWriter(writer);
  }

  @Test
  public void testNoThreshold() throws Exception {
    RecordWriterManager mgr = managerBuilder()
      .cutOffSecs(10)
      .cutOffSizeBytes(0)
      .cutOffRecords(0)
      .build();

    Date now = getFixedDate();

    Date recordDate = now;
    Record record = RecordCreator.create();
    record.set(Field.create("a"));
    RecordWriter writer = mgr.getWriter(now, recordDate, record);
    Assert.assertNotNull(writer);
    for (int i = 0; i < 10; i++) {
      Assert.assertFalse(mgr.isOverThresholds(writer));
      writer.write(record);
      writer.flush();
    }
    Assert.assertFalse(mgr.isOverThresholds(writer));
    mgr.commitWriter(writer);
  }

  @Test
  public void testIncrementDate() throws Exception {
    RecordWriterManager mgr = managerBuilder()
      .cutOffSecs(0)
      .build();

    Date date = new Date();
    Date inc = mgr.incrementDate(date, Calendar.HOUR);
    Assert.assertEquals(TimeUnit.HOURS.toMillis(1), inc.getTime() - date.getTime());
  }

  @Test
  public void testCreateGlobs() throws Exception {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.setTime(Utils.parse("2015-05-07T12:35Z"));

    RecordWriterManager mgr = managerBuilder().dirPathTemplate("/foo").build();
    Assert.assertEquals("/foo/" + mgr.getTempFileName(), mgr.createGlob(calendar));

    mgr = managerBuilder().dirPathTemplate("/foo/${YYYY()}/${YY()}/${MM()}/${DD()}/${hh()}/${mm()}/${ss()}").build();
    Assert.assertEquals("/foo/2015/15/05/07/12/35/00/" + mgr.getTempFileName(), mgr.createGlob(calendar));

    mgr = managerBuilder().dirPathTemplate("/foo/${YYYY()}/${record:value('/foo')}").build();
    Assert.assertEquals("/foo/2015/*/" + mgr.getTempFileName(), mgr.createGlob(calendar));
  }

  @Test
  public void testGetGlobsAndCommitOldFiles() throws Exception {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.add(Calendar.HOUR, -2);
    Date lastBatch = calendar.getTime();
    ContextInfoCreator.setLastBatch(targetContext, lastBatch.getTime());

    calendar.add(Calendar.HOUR, -1);
    Date beforeLastBatchWithinCutOff = calendar.getTime();

    calendar.add(Calendar.DATE, -1);
    Date beforeLastBatchOutsideCutOff = calendar.getTime();

    calendar.add(Calendar.DATE, 2);
    Date future = calendar.getTime();

    File testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir.mkdirs());

    // using 1 hour cutoff
    RecordWriterManager mgr = managerBuilder()
      .dirPathTemplate(testDir.getAbsolutePath() + "/${YY()}_${MM()}_${DD()}_${hh()}/${record:value('/')}")
      .cutOffSecs(3600)
      .build();

    //this one should not show up when globing
    String f1 = createTempFile(mgr, beforeLastBatchOutsideCutOff, "a");

    //all this should show up when globing
    String f2 = createTempFile(mgr, beforeLastBatchWithinCutOff, "b");
    String f3 = createTempFile(mgr, beforeLastBatchWithinCutOff, "c");
    String f4 = createTempFile(mgr, lastBatch, "d");

    //this one should not show up when globing
    String f5 = createTempFile(mgr, future, "e");

    Set<String> expected = ImmutableSet.of(f2, f3, f4);

    Set<String> got = new HashSet<>();
    URI uri = new URI("file:///");
    Configuration conf = new HdfsConfiguration();
    FileSystem fs = FileSystem.get(uri, conf);

    // verifying getGlobs() returned are within the search boundaries
    List<String> globs = mgr.getGlobs();
    for (String glob : globs) {
      FileStatus[] status = fs.globStatus(new Path("file://" + glob));
      for (FileStatus s : status) {
        got.add(s.getPath().toString().substring("file:".length()));
      }
    }
    Assert.assertEquals(expected, got);

    // committing all temps within search boundaries
    mgr.commitOldFiles(fs);

    // verifying there are not temps within search boundaries after committing
    for (String glob : globs) {
      FileStatus[] status = fs.globStatus(new Path("file://" + glob));
      for (FileStatus s : status) {
        Assert.fail();
      }
    }

    // verifying temps outside boundaries are still there
    Assert.assertTrue(new File(f1).exists());
    Assert.assertTrue(new File(f5).exists());
  }

  @Test
  public void testShouldRoll() throws Exception {
    File testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir.mkdirs());


    Record negativeRecord = RecordCreator.create();
    negativeRecord.set(Field.create("a"));

    Record positiveRecord = RecordCreator.create();
    positiveRecord.getHeader().setAttribute("roll", "yes please");
    positiveRecord.set(Field.create("a"));

    RecordWriterManager negativeManager = managerBuilder().build();

    RecordWriter negativeRecordWriter = negativeManager.getWriter(new Date(), new Date(), negativeRecord);
    Assert.assertFalse(negativeManager.shouldRoll(negativeRecordWriter, negativeRecord));

    RecordWriter positiveRecordWriter = negativeManager.getWriter(new Date(), new Date(), positiveRecord);
    Assert.assertFalse(negativeManager.shouldRoll(positiveRecordWriter, positiveRecord));

    RecordWriterManager positiveManager = managerBuilder().rollHeaderName("roll").rollIfHeader(true).build();

    negativeRecordWriter = positiveManager.getWriter(new Date(), new Date(), negativeRecord);
    Assert.assertFalse(positiveManager.shouldRoll(negativeRecordWriter, negativeRecord));

    positiveRecordWriter = positiveManager.getWriter(new Date(), new Date(), positiveRecord);
    Assert.assertTrue(positiveManager.shouldRoll(positiveRecordWriter, positiveRecord));
  }

  @Test
  public void testHandleAlreadyExistingFiles() throws Exception {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.add(Calendar.HOUR, -2);
    Date lastBatch = calendar.getTime();
    ContextInfoCreator.setLastBatch(targetContext, lastBatch.getTime());

    File testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir.mkdirs());

    final String dirTemp = "/${YY()}_${MM()}_${DD()}_${hh()}/${sdc:hostname()}/${record:value('/y')}${record:value('/z')}${str:concat('', '')}/bar/";
    // using 1 hour cutoff
    RecordWriterManager mgr = managerBuilder()
        .dirPathTemplate(testDir.getAbsolutePath() + dirTemp)
        .uniquePrefix("sdc-c27f92a7-2162-46ef-af84-bce6db4255cf")
        .cutOffSecs(3600)
        .build();

    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("y", Field.create("y0"));
    map.put("z", Field.create("z0"));
    record.set(Field.create(map));
    String f1 = createTempFile(mgr, lastBatch, record);

    map.put("y", Field.create(""));
    map.put("z", Field.create(""));
    record.set(Field.create(map));
    String f2 = createTempFile(mgr, lastBatch, record);

    map.put("y", Field.create("y0/y1"));
    map.put("z", Field.create("z0/z1"));
    record.set(Field.create(map));
    String f3 = createTempFile(mgr, lastBatch, record);


    // the temp files under f1 will be renamed, under f2 & f3 will not be renamed
    final int totalTempFiles = 1;
    int ret = mgr.handleAlreadyExistingFiles();

    Assert.assertEquals(totalTempFiles, ret);
  }

  private String createTempFile(RecordWriterManager mgr, Date date, String subDir) throws Exception {
    String path = mgr.getDirPath(date, RecordCreator.create());
    path += "/" + subDir + "/";
    Files.createDirectories(Paths.get(path));
    return Files.createFile(Paths.get(path + mgr.getTempFileName())).toString();
  }

  private String createTempFile(RecordWriterManager mgr, Date date, Record record) throws Exception {
    String path = mgr.getDirPath(date, record);
    Files.createDirectories(Paths.get(path));
    return Files.createFile(Paths.get(path + mgr.getTempFileName())).toString();
  }
}
