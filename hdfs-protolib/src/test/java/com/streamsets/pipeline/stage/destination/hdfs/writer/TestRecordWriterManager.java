/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs.writer;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsDTarget;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsFileType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;

public class TestRecordWriterManager {
  private static Path testDir;
  private static Target.Context targetContext = ContextInfoCreator.createTargetContext(HdfsDTarget.class,
    "testWritersLifecycle", false, OnRecordError.TO_ERROR);


  public static class DummyDataGeneratorFactory extends DataGeneratorFactory {
    protected DummyDataGeneratorFactory(Settings settings) {
      super(settings);
    }

    @Override
    public DataGenerator getGenerator(OutputStream os) throws IOException, DataGeneratorException {
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
    URI uri = new URI("file:///");
    Configuration conf = new HdfsConfiguration();
    String prefix = "prefix";
    String template = getTestDir().toString() +
                      "/${YYYY()}/${YY()}/${MM()}/${DD()}/${hh()}/${mm()}/${ss()}/" +
                      "${not empty record:value('/') ? record:value('/') : 'blah'}";
    TimeZone timeZone = TimeZone.getTimeZone("UTC");
    long cutOffSecs = 10;
    long cutOffSize = 20;
    long cutOffRecords = 2;
    HdfsFileType fileType = HdfsFileType.TEXT;
    CompressionCodec compressionCodec = null;
    SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.NONE;
    String keyEL = "uuid()";
    DataGeneratorFactory generatorFactory = new DummyDataGeneratorFactory(null);
    RecordWriterManager mgr = new RecordWriterManager(uri, conf, prefix, template, timeZone, cutOffSecs, cutOffSize,
      cutOffRecords, fileType, compressionCodec, compressionType, keyEL, generatorFactory, targetContext);

    Date date = getFixedDate();

    Record record = RecordCreator.create();
    record.set(Field.create("a"));
    Assert.assertEquals(getTestDir() + "/2015/15/01/20/09/56/01/a", mgr.getDirPath(date, record));

    record.set(null);
    Assert.assertEquals(getTestDir() + "/2015/15/01/20/09/56/01/blah", mgr.getDirPath(date, record));

    Date now = date;
    Date recordDate = new Date(now.getTime() - 5000);
    // taking into account the ceiling
    Assert.assertEquals(5999, mgr.getTimeToLiveMillis(now, recordDate));
  }

  @Test
  public void testNoCompressionCodec() throws Exception {
    testPath(null);
  }

  @Test
  public void testCompressionCodec() throws Exception {
    testPath(new SnappyCodec());
  }

  private void testPath(CompressionCodec compressionCodec) throws Exception {
    URI uri = new URI("file:///");
    Configuration conf = new HdfsConfiguration();
    String prefix = "prefix";
    String template = getTestDir().toString() + "/${YYYY()}/${YY()}/${MM()}/${DD()}/${hh()}/${mm()}/${ss()}/${record:value('/')}";
    TimeZone timeZone = TimeZone.getTimeZone("UTC");
    long cutOffSecs = 10;
    long cutOffSize = 20;
    long cutOffRecords = 2;
    HdfsFileType fileType = HdfsFileType.TEXT;
    SequenceFile.CompressionType compressionType = (compressionCodec == null) ? SequenceFile.CompressionType.NONE
                                                                              : SequenceFile.CompressionType.BLOCK;
    String keyEL = "uuid()";
    DataGeneratorFactory generatorFactory = new DummyDataGeneratorFactory(null);
    RecordWriterManager mgr = new RecordWriterManager(uri, conf, prefix, template, timeZone, cutOffSecs, cutOffSize,
      cutOffRecords, fileType, compressionCodec, compressionType,keyEL, generatorFactory, targetContext);

    if (compressionCodec == null) {
      Assert.assertEquals("", mgr.getExtension());
    } else {
      Assert.assertEquals(compressionCodec.getDefaultExtension(), mgr.getExtension());
    }

    Date date = getFixedDate();
    Record record = RecordCreator.create();
    record.set(Field.create("a"));
    Assert.assertEquals(new Path(getTestDir(), "2015/15/01/20/09/56/01/a/_" + prefix + "_tmp" + mgr.getExtension()),
                        mgr.getPath(date, record));
  }

  private void testTextFile(CompressionCodec compressionCodec) throws Exception {
    URI uri = new URI("file:///");
    Configuration conf = new HdfsConfiguration();
    String prefix = "prefix";
    String template = getTestDir().toString() + "/${YYYY()}";
    TimeZone timeZone = TimeZone.getTimeZone("UTC");
    long cutOffSecs = 10;
    long cutOffSize = 20;
    long cutOffRecords = 2;
    HdfsFileType fileType = HdfsFileType.TEXT;
    SequenceFile.CompressionType compressionType = null;
    String keyEL = null;
    DataGeneratorFactory generatorFactory = new DummyDataGeneratorFactory(null);
    RecordWriterManager mgr = new RecordWriterManager(uri, conf, prefix, template, timeZone, cutOffSecs, cutOffSize,
      cutOffRecords, fileType, compressionCodec, compressionType, keyEL, generatorFactory, targetContext);
    FileSystem fs = FileSystem.get(uri, conf);
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
    URI uri = new URI("file:///");
    Configuration conf = new HdfsConfiguration();
    String prefix = "prefix";
    String template = getTestDir().toString() + "/${YYYY()}";
    TimeZone timeZone = TimeZone.getTimeZone("UTC");
    long cutOffSecs = 10;
    long cutOffSize = 20;
    long cutOffRecords = 2;
    HdfsFileType fileType = HdfsFileType.SEQUENCE_FILE;
    String keyEL = "${uuid()}";
    DataGeneratorFactory generatorFactory = new DummyDataGeneratorFactory(null);
    RecordWriterManager mgr = new RecordWriterManager(uri, conf, prefix, template, timeZone, cutOffSecs, cutOffSize,
      cutOffRecords, fileType, compressionCodec, compressionType, keyEL, generatorFactory, targetContext);
    FileSystem fs = FileSystem.get(uri, conf);
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
    URI uri = new URI("file:///");
    Configuration conf = new HdfsConfiguration();
    String prefix = "prefix";
    String template = getTestDir().toString() + "/${YYYY()}/${MM()}/${DD()}/${hh()}/${mm()}/${ss()}/${record:value('/')}";
    TimeZone timeZone = TimeZone.getTimeZone("UTC");
    long cutOffSecs = 10;
    long cutOffSize = 5;
    long cutOffRecords = 2;
    HdfsFileType fileType = HdfsFileType.TEXT;
    DefaultCodec compressionCodec = new DefaultCodec();
    compressionCodec.setConf(conf);
    SequenceFile.CompressionType compressionType = null;
    String keyEL = null;
    DataGeneratorFactory generatorFactory = new DummyDataGeneratorFactory(null);
    RecordWriterManager mgr = new RecordWriterManager(uri, conf, prefix, template, timeZone, cutOffSecs, cutOffSize,
      cutOffRecords, fileType, compressionCodec , compressionType, keyEL, generatorFactory, targetContext);

    FileSystem fs = FileSystem.get(uri, conf);
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
    Assert.assertTrue(fs.exists(finalPath));
    Assert.assertTrue(finalPath.getName().equals(prefix + "-000000" + compressionCodec.getDefaultExtension()));

    // record qualifies, second file
    writer = mgr.getWriter(now, recordDate, record);
    finalPath = mgr.commitWriter(writer);
    Assert.assertTrue(fs.exists(finalPath));
    Assert.assertTrue(finalPath.getName().equals(prefix + "-000001" + compressionCodec.getDefaultExtension()));

    // record qualifies, leaving temp file
    writer = mgr.getWriter(now, recordDate, record);
    writer.close();

    // record qualifies, it should roll temp file and create 4th file
    writer = mgr.getWriter(now, recordDate, record);
    finalPath = mgr.commitWriter(writer);
    Assert.assertFalse(fs.exists(tempPath));
    Assert.assertTrue(fs.exists(finalPath));
    Assert.assertTrue(fs.exists(new Path(finalPath.getParent(),
                                         prefix + "-000002" + compressionCodec.getDefaultExtension())));
    Assert.assertTrue(finalPath.getName().equals(prefix + "-000003" + compressionCodec.getDefaultExtension()));

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


  @Test
  public void testThresholdRecords() throws Exception {
    URI uri = new URI("file:///");
    Configuration conf = new HdfsConfiguration();
    String prefix = "prefix";
    String template = getTestDir().toString() + "/${YYYY()}/${MM()}/${DD()}/${hh()}/${mm()}/${ss()}/${record:value('/')}";
    TimeZone timeZone = TimeZone.getTimeZone("UTC");
    long cutOffSecs = 10;
    long cutOffSize = 50000;
    long cutOffRecords = 2;
    HdfsFileType fileType = HdfsFileType.TEXT;
    DefaultCodec compressionCodec = new DefaultCodec();
    compressionCodec.setConf(conf);
    SequenceFile.CompressionType compressionType = null;
    String keyEL = null;
    DataGeneratorFactory generatorFactory = new DummyDataGeneratorFactory(null);
    RecordWriterManager mgr = new RecordWriterManager(uri, conf, prefix, template, timeZone, cutOffSecs, cutOffSize,
      cutOffRecords, fileType, compressionCodec , compressionType, keyEL, generatorFactory, targetContext);

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
    URI uri = new URI("file:///");
    Configuration conf = new HdfsConfiguration();
    String prefix = "prefix";
    String template = getTestDir().toString() + "/${YYYY()}/${MM()}/${DD()}/${hh()}/${mm()}/${ss()}/${record:value('/')}";
    TimeZone timeZone = TimeZone.getTimeZone("UTC");
    long cutOffSecs = 10;
    long cutOffSize = 4;
    long cutOffRecords = 20;
    HdfsFileType fileType = HdfsFileType.TEXT;
    DefaultCodec compressionCodec = new DefaultCodec();
    compressionCodec.setConf(conf);
    SequenceFile.CompressionType compressionType = null;
    String keyEL = null;
    DataGeneratorFactory generatorFactory = new DummyDataGeneratorFactory(null);
    RecordWriterManager mgr = new RecordWriterManager(uri, conf, prefix, template, timeZone, cutOffSecs, cutOffSize,
      cutOffRecords, fileType, compressionCodec , compressionType, keyEL, generatorFactory, targetContext);
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
    URI uri = new URI("file:///");
    Configuration conf = new HdfsConfiguration();
    String prefix = "prefix";
    String template = getTestDir().toString() + "/${YYYY()}/${MM()}/${DD()}/${hh()}/${mm()}/${ss()}/${record:value('/')}";
    TimeZone timeZone = TimeZone.getTimeZone("UTC");
    long cutOffSecs = 10;
    long cutOffSize = 0;
    long cutOffRecords = 0;
    HdfsFileType fileType = HdfsFileType.TEXT;
    DefaultCodec compressionCodec = new DefaultCodec();
    compressionCodec.setConf(conf);
    SequenceFile.CompressionType compressionType = null;
    String keyEL = null;
    DataGeneratorFactory generatorFactory = new DummyDataGeneratorFactory(null);
    RecordWriterManager mgr = new RecordWriterManager(uri, conf, prefix, template, timeZone, cutOffSecs, cutOffSize,
      cutOffRecords, fileType, compressionCodec , compressionType, keyEL, generatorFactory, targetContext);
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
  public void testCeilingDate() throws Exception {
    URI uri = new URI("file:///");
    Configuration conf = new HdfsConfiguration();
    String prefix = "prefix";
    TimeZone timeZone = TimeZone.getTimeZone("UTC");
    long cutOffSecs = 10;
    long cutOffSize = 5;
    long cutOffRecords = 2;
    HdfsFileType fileType = HdfsFileType.TEXT;
    DefaultCodec compressionCodec = new DefaultCodec();
    compressionCodec.setConf(conf);
    SequenceFile.CompressionType compressionType = null;
    String keyEL = null;
    DataGeneratorFactory generatorFactory = new DummyDataGeneratorFactory(null);

    // up to seconds
    String template = getTestDir().toString() + "/${YYYY()}/${MM()}/${DD()}/${hh()}/${mm()}/${ss()}";
    RecordWriterManager mgr = new RecordWriterManager(uri, conf, prefix, template, timeZone, cutOffSecs, cutOffSize,
      cutOffRecords, fileType, compressionCodec , compressionType, keyEL, generatorFactory, targetContext);

    Date actual = parseDate("2015-01-20T14:01:15Z");
    Date expected = new Date(parseDate("2015-01-20T14:01:16Z").getTime() - 1);
    Date computed = mgr.getCeilingDateBasedOnTemplate(template, TimeZone.getDefault(), actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());
    actual = new Date(actual.getTime() + 10);
    computed = mgr.getCeilingDateBasedOnTemplate(template, TimeZone.getDefault(), actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());

    // up to minutes
    template = getTestDir().toString() + "/${YYYY()}/${MM()}/${DD()}/${hh()}/${mm()}";
    mgr = new RecordWriterManager(uri, conf, prefix, template, timeZone, cutOffSecs, cutOffSize,
      cutOffRecords, fileType, compressionCodec , compressionType, keyEL, generatorFactory, targetContext);

    actual = parseDate("2015-01-20T14:01:15Z");
    expected = new Date(parseDate("2015-01-20T14:02:00Z").getTime() - 1);
    computed = mgr.getCeilingDateBasedOnTemplate(template, TimeZone.getDefault(), actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());
    actual = new Date(actual.getTime() + 10);
    computed = mgr.getCeilingDateBasedOnTemplate(template, TimeZone.getDefault(), actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());

    // up to hours
    template = getTestDir().toString() + "/${YYYY()}/${MM()}/${DD()}/${hh()}";
    mgr = new RecordWriterManager(uri, conf, prefix, template, timeZone, cutOffSecs, cutOffSize,
      cutOffRecords, fileType, compressionCodec , compressionType, keyEL, generatorFactory, targetContext);

    actual = parseDate("2015-01-20T14:01:15Z");
    expected = new Date(parseDate("2015-01-20T15:00:00Z").getTime() - 1);
    computed = mgr.getCeilingDateBasedOnTemplate(template, TimeZone.getDefault(), actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());
    actual = new Date(actual.getTime() + 10);
    computed = mgr.getCeilingDateBasedOnTemplate(template, TimeZone.getDefault(), actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());

    // up to days
    template = getTestDir().toString() + "/${YYYY()}/${MM()}/${DD()}";
    mgr = new RecordWriterManager(uri, conf, prefix, template, timeZone, cutOffSecs, cutOffSize,
      cutOffRecords, fileType, compressionCodec , compressionType, keyEL, generatorFactory, targetContext);

    actual = parseDate("2015-01-20T14:01:15Z");
    expected = new Date(parseDate("2015-01-20T24:00:00Z").getTime() - 1);
    computed = mgr.getCeilingDateBasedOnTemplate(template, TimeZone.getTimeZone("UTC"), actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());
    actual = new Date(actual.getTime() + 10);
    computed = mgr.getCeilingDateBasedOnTemplate(template, TimeZone.getTimeZone("UTC"), actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());

    // up to months
    template = getTestDir().toString() + "/${YYYY()}/${MM()}";
    mgr = new RecordWriterManager(uri, conf, prefix, template, timeZone, cutOffSecs, cutOffSize,
      cutOffRecords, fileType, compressionCodec , compressionType, keyEL, generatorFactory, targetContext);
    actual = parseDate("2015-01-20T14:01:15Z");
    expected = new Date(parseDate("2015-01-31T24:00:00Z").getTime() - 1);
    computed = mgr.getCeilingDateBasedOnTemplate(template, TimeZone.getTimeZone("UTC"), actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());
    actual = new Date(actual.getTime() + 10);
    computed = mgr.getCeilingDateBasedOnTemplate(template, TimeZone.getTimeZone("UTC"), actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());

    // up to years
    template = getTestDir().toString() + "/${YYYY()}";
    mgr = new RecordWriterManager(uri, conf, prefix, template, timeZone, cutOffSecs, cutOffSize,
      cutOffRecords, fileType, compressionCodec , compressionType, keyEL, generatorFactory, targetContext);

    actual = parseDate("2015-01-20T14:01:15Z");
    expected = new Date(parseDate("2015-12-31T24:00:00Z").getTime() - 1);
    computed = mgr.getCeilingDateBasedOnTemplate(template, TimeZone.getTimeZone("UTC"), actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());
    actual = new Date(actual.getTime() + 10);
    computed = mgr.getCeilingDateBasedOnTemplate(template, TimeZone.getTimeZone("UTC"), actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());

    // leap year
    template = getTestDir().toString() + "/${YY()}/${MM()}";
    mgr = new RecordWriterManager(uri, conf, prefix, template, timeZone, cutOffSecs, cutOffSize,
      cutOffRecords, fileType, compressionCodec , compressionType, keyEL, generatorFactory, targetContext);

    actual = parseDate("2016-02-20T1:01:15Z");
    expected = new Date(parseDate("2016-03-01T00:00:00Z").getTime() - 1);
    computed = mgr.getCeilingDateBasedOnTemplate(template, TimeZone.getTimeZone("UTC"), actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());
    actual = new Date(actual.getTime() + 10);
    computed = mgr.getCeilingDateBasedOnTemplate(template, TimeZone.getTimeZone("UTC"), actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());

    // no date at all
    template = getTestDir().toString() + "/foo";
    mgr = new RecordWriterManager(uri, conf, prefix, template, timeZone, cutOffSecs, cutOffSize,
                                  cutOffRecords, fileType, compressionCodec , compressionType, keyEL,
                                  generatorFactory, targetContext);

    actual = parseDate("2015-01-20T14:01:15Z");
    computed = mgr.getCeilingDateBasedOnTemplate(template, TimeZone.getTimeZone("UTC"), actual);
    Assert.assertNull(computed);

  }

  private void testDirTemplate(String template) throws Exception {
    URI uri = new URI("file:///");
    Configuration conf = new HdfsConfiguration();
    String prefix = "prefix";
    TimeZone timeZone = TimeZone.getTimeZone("UTC");
    long cutOffSecs = 10;
    long cutOffSize = 5;
    long cutOffRecords = 2;
    HdfsFileType fileType = HdfsFileType.TEXT;
    DefaultCodec compressionCodec = new DefaultCodec();
    compressionCodec.setConf(conf);
    SequenceFile.CompressionType compressionType = null;
    String keyEL = null;
    DataGeneratorFactory generatorFactory = new DummyDataGeneratorFactory(null);

    RecordWriterManager mgr = new RecordWriterManager(uri, conf, prefix, template, timeZone, cutOffSecs, cutOffSize,
      cutOffRecords, fileType, compressionCodec , compressionType, keyEL, generatorFactory, targetContext);
    mgr.getCeilingDateBasedOnTemplate(template, TimeZone.getDefault(), new Date());
  }

  @Test
  public void testNoTimeFunctions() throws Exception {
    testDirTemplate("/");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidDirTemplateGapInTokens1() throws Exception {
    testDirTemplate("${MM()}");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidDirTemplateGapInTokens2() throws Exception {
    testDirTemplate("${YY()}/${DD()}");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidDirTemplateGapInTokens3() throws Exception {
    testDirTemplate("${YY()}/${MM()}/${hh()}");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidDirTemplateGapInTokens4() throws Exception {
    testDirTemplate("${YY()}/${MM()}/${DD()}/${mm()}");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidDirTemplateGapInTokens5() throws Exception {
    testDirTemplate("${YY()}/${MM()}/${DD()}/${hh()}/${ss()}");
  }

}
