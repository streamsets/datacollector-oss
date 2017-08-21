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

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsDTarget;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestRecordWriter {
  private static Path testDir;

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

  private FileSystem getRawLocalFileSystem() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass("fs.file.impl", RawLocalFileSystem.class, FileSystem.class);
    return FileSystem.get(new URI("file:///"), conf);
  }

  @Test
  public void testTextFile() throws Exception {
    FileSystem fs = getRawLocalFileSystem();
    try {
      Path file = new Path(getTestDir(), "file.txt");
      OutputStream os = fs.create(file, false);
      long timeToLive = 10000;
      long expires = System.currentTimeMillis() + timeToLive;
      RecordWriter writer = new RecordWriter(file, timeToLive, os, new DummyDataGeneratorFactory(null), null);
      Assert.assertTrue(writer.isTextFile());
      Assert.assertFalse(writer.isSeqFile());
      Assert.assertEquals(file, writer.getPath());
      Assert.assertTrue(expires <= writer.getExpiresOn());
      Assert.assertTrue(writer.toString().contains(file.toString()));
      Record record = RecordCreator.create();
      record.set(Field.create("a"));
      writer.write(record);
      record.set(Field.create("z"));
      writer.write(record);
      Assert.assertFalse(writer.isClosed());
      writer.flush();
      Assert.assertTrue(writer.getLength() > 2);
      Assert.assertEquals(2, writer.getRecords());
      writer.close();
      Assert.assertTrue(writer.isClosed());
      try {
        writer.write(record);
        Assert.fail();
      } catch (IOException ex) {
        //NOP
      }
      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file)));
      Assert.assertEquals("a", reader.readLine());
      Assert.assertEquals("z", reader.readLine());
      Assert.assertNull(reader.readLine());
      reader.close();
    } finally {
      fs.close();
    }
  }

  private void testSequenceFile(boolean useUUIDAsKey) throws Exception {
    String keyEL = (useUUIDAsKey) ? "${uuid()}" : "${record:value('/')}";
    FileSystem fs = getRawLocalFileSystem();
    try {
      Path file = new Path(getTestDir(), "file.txt");

      SequenceFile.Writer seqFile = SequenceFile.createWriter(fs, new HdfsConfiguration(), file, Text.class,
                                                              Text.class, SequenceFile.CompressionType.NONE,
                                                              (CompressionCodec) null);
      long timeToLive = 10000;
      long expires = System.currentTimeMillis() + timeToLive;
      RecordWriter writer = new RecordWriter(file, timeToLive, seqFile, keyEL, new DummyDataGeneratorFactory(null),
        ContextInfoCreator.createTargetContext(HdfsDTarget.class, "testWritersLifecycle", false,
          OnRecordError.TO_ERROR, null));
      Assert.assertFalse(writer.isTextFile());
      Assert.assertTrue(writer.isSeqFile());
      Assert.assertEquals(file, writer.getPath());
      Assert.assertTrue(expires <= writer.getExpiresOn());
      Assert.assertTrue(writer.toString().contains(file.toString()));
      Record record = RecordCreator.create();
      record.set(Field.create("a"));
      writer.write(record);
      record.set(Field.create("z"));
      writer.write(record);
      Assert.assertFalse(writer.isClosed());
      writer.flush();
      Assert.assertTrue(writer.getLength() > 4);
      Assert.assertEquals(2, writer.getRecords());
      writer.close();
      Assert.assertTrue(writer.isClosed());
      try {
        writer.write(record);
        Assert.fail();
      } catch (IOException ex) {
        //NOP
      }
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, new HdfsConfiguration());
      Text key = new Text();
      Text value = new Text();
      Assert.assertTrue(reader.next(key, value));
      if (useUUIDAsKey) {
        Assert.assertNotNull(UUID.fromString(key.toString()));
      } else {
        Assert.assertEquals("a", key.toString());
      }
      Assert.assertEquals("a", value.toString().trim());
      Assert.assertTrue(reader.next(key, value));
      if (useUUIDAsKey) {
        Assert.assertNotNull(UUID.fromString(key.toString()));

      } else {
        Assert.assertEquals("z", key.toString());
      }
      Assert.assertEquals("z", value.toString().trim());
      Assert.assertFalse(reader.next(key, value));
      reader.close();
    } finally {
      fs.close();
    }
  }

  @Test
  public void testSequenceFileUsingFieldAsKey() throws Exception {
    testSequenceFile(false);
  }

  @Test
  public void testSequenceFileUsingUUIDAsKey() throws Exception {
    testSequenceFile(true);
  }

  @Test
  public void testTextFilesWriterHFlush() throws Exception {
    FileSystem fs = getRawLocalFileSystem();
    final AtomicBoolean hflushCalled = new AtomicBoolean(false);
    try {
      Path file = new Path(getTestDir(), "testTexFilesWriterHFlush.txt");
      FSDataOutputStream fsDataOutputStream = Mockito.spy(fs.create(file, false));
      OutputStream os = new HflushableWrapperOutputStream(fsDataOutputStream);

      //Whenever hflush is called set hflushCalled to true.
      Mockito.doAnswer(new Answer() {
        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
          hflushCalled.compareAndSet(false, true);
          return invocationOnMock.callRealMethod();
        }
      }).when(fsDataOutputStream).hflush();

      RecordWriter writer = new RecordWriter(file, 1000, os, new DummyDataGeneratorFactory(null), null);
      Record record = RecordCreator.create();
      record.set(Field.create("a"));
      writer.write(record);
      //hflush should be called.
      writer.flush();
      //hflushCalled should be true.
      Assert.assertTrue(hflushCalled.get());
      writer.close();
    } finally {
      fs.close();
    }
  }

}
