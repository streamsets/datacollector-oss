/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs.writer;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.generator.CharDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Writer;
import java.util.UUID;

public class TestRecordWriter {
  private static Path testDir;

  public static class DummyDataGeneratorFactory extends CharDataGeneratorFactory {
    @Override
    public DataGenerator getGenerator(final Writer writer) throws IOException, DataGeneratorException {
      return new DataGenerator() {
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
      };
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

  @Test
  public void testTextFile() throws Exception {
    FileSystem fs = FileSystem.getLocal(new HdfsConfiguration());
    try {
      Path file = new Path(getTestDir(), "file.txt");
      OutputStream os = fs.create(file, false);
      long timeToLive = 10000;
      long expires = System.currentTimeMillis() + timeToLive;
      RecordWriter writer = new RecordWriter(file, timeToLive, os, new DummyDataGeneratorFactory());
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
    FileSystem fs = FileSystem.getLocal(new HdfsConfiguration());
    try {
      Path file = new Path(getTestDir(), "file.txt");

      SequenceFile.Writer seqFile = SequenceFile.createWriter(fs, new HdfsConfiguration(), file, Text.class,
                                                              Text.class, SequenceFile.CompressionType.NONE,
                                                              (CompressionCodec) null);
      long timeToLive = 10000;
      long expires = System.currentTimeMillis() + timeToLive;
      RecordWriter writer = new RecordWriter(file, timeToLive, seqFile, keyEL, new DummyDataGeneratorFactory());
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

}
