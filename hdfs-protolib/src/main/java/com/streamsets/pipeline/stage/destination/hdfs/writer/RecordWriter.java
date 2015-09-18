/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class RecordWriter {
  private final static Logger LOG = LoggerFactory.getLogger(RecordWriter.class);
  private final static boolean IS_TRACE_ENABLED = LOG.isTraceEnabled();
  private final long expires;
  private final Path path;
  private final DataGeneratorFactory generatorFactory;
  private long recordCount;

  private CountingOutputStream textOutputStream;
  private DataGenerator generator;
  private boolean textFile;

  private SequenceFile.Writer seqWriter;
  private String keyEL;
  private ELEval keyElEval;
  private ELVars elVars;
  private Text key;
  private Text value;
  private boolean seqFile;

  private RecordWriter(Path path, long timeToLiveMillis, DataGeneratorFactory generatorFactory) {
    this.expires = (timeToLiveMillis == Long.MAX_VALUE) ? timeToLiveMillis : System.currentTimeMillis() + timeToLiveMillis;
    this.path = path;
    this.generatorFactory = generatorFactory;
    LOG.debug("Path[{}] - Creating", path);
  }

  public RecordWriter(Path path, long timeToLiveMillis, OutputStream textOutputStream,
      DataGeneratorFactory generatorFactory) throws StageException, IOException {
    this(path, timeToLiveMillis, generatorFactory);
    this.textOutputStream = new CountingOutputStream(textOutputStream);
    generator = generatorFactory.getGenerator(this.textOutputStream);
    textFile = true;
  }

  public RecordWriter(Path path, long timeToLiveMillis, SequenceFile.Writer seqWriter, String keyEL,
      DataGeneratorFactory generatorFactory, Target.Context context) {
    this(path, timeToLiveMillis, generatorFactory);
    this.seqWriter = seqWriter;
    this.keyEL = keyEL;
    keyElEval = context.createELEval("keyEl");
    elVars = context.createELVars();
    key = new Text();
    value = new Text();
    seqFile = true;
  }

  public Path getPath() {
    return path;
  }

  public long getExpiresOn() {
    return expires;
  }

  public void write(Record record) throws IOException, StageException {
    if (IS_TRACE_ENABLED) {
      LOG.trace("Path[{}] - Writing ['{}']", path, record.getHeader().getSourceId());
    }
    if (generator != null) {
      generator.write(record);
    } else if (seqWriter != null) {
      RecordEL.setRecordInContext(elVars, record);
      key.set(keyElEval.eval(elVars, keyEL, String.class));
      ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
      DataGenerator dg = generatorFactory.getGenerator(baos);
      dg.write(record);
      dg.close();
      value.set(new String(baos.toByteArray()));
      seqWriter.append(key, value);
    } else {
      throw new IOException(Utils.format("RecordWriter '{}' is closed", path));
    }
    recordCount++;
  }

  public void flush() throws IOException {
    if (IS_TRACE_ENABLED) {
      LOG.trace("Path[{}] - Flushing", path);
    }
    if (generator != null) {
      generator.flush();
    } else if (seqWriter != null) {
      seqWriter.hflush();
    }
  }

  // due to buffering of underlying streams, the reported length may be less than the actual one up to the
  // buffer size.
  public long getLength() throws IOException {
    long length = -1;
    if (generator != null) {
      length = textOutputStream.getCount();
    } else if (seqWriter != null) {
      length = seqWriter.getLength();
    }
    return length;
  }

  public long getRecords() {
    return recordCount;
  }

  public void close() throws IOException {
    LOG.debug("Path[{}] - Closing", path);
    try {
      if (generator != null) {
        generator.close();
      } else if (seqWriter != null) {
        seqWriter.close();
      }
    } finally {
      generator = null;
      seqWriter = null;
    }
  }

  public boolean isTextFile() {
    return textFile;
  }

  public boolean isSeqFile() {
    return seqFile;
  }

  public boolean isClosed() {
    return generator == null && seqWriter == null;
  }

  @Override
  public String toString() {
    return Utils.format("RecordWriter[path='{}']", path);
  }

}
