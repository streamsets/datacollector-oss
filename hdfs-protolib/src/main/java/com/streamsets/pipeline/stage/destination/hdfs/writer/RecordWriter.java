/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs.writer;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.lib.recordserialization.RecordToString;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Method;
import java.util.UUID;

public class RecordWriter {
  private final static Logger LOG = LoggerFactory.getLogger(RecordWriter.class);

  private static final Method UUID_FUNC;

  static {
    try {
      UUID_FUNC = RecordWriter.class.getMethod("UUIDFunc");
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static String UUIDFunc() {
    return UUID.randomUUID().toString();
  }

  private final long expires;
  private final Path path;
  private final RecordToString recordToString;
  private long recordCount;

  private CountingOutputStream textOutputStream;
  private Writer writer;
  private boolean textFile;

  private SequenceFile.Writer seqWriter;
  private String keyEL;
  private ELEvaluator elEval;
  private ELEvaluator.Variables elVars;
  private Text key;
  private Text value;
  private boolean seqFile;

  private RecordWriter(Path path, long timeToLiveMillis, RecordToString recordToString) {
    this.expires = System.currentTimeMillis() + timeToLiveMillis;
    this.path = path;
    this.recordToString = recordToString;
    LOG.debug("Path[{}] - Creating", path);
  }

  public RecordWriter(Path path, long timeToLiveMillis, OutputStream textOutputStream, RecordToString recordToString) {
    this(path, timeToLiveMillis, recordToString);
    this.textOutputStream = new CountingOutputStream(textOutputStream);
    writer = new OutputStreamWriter(this.textOutputStream);
    textFile = true;
  }

  public RecordWriter(Path path, long timeToLiveMillis, SequenceFile.Writer seqWriter, String keyEL,
      RecordToString recordToString) {
    this(path, timeToLiveMillis, recordToString);
    this.seqWriter = seqWriter;
    this.keyEL = keyEL;
    elEval = new ELEvaluator();
    elEval.registerFunction("", "uuid", UUID_FUNC);
    ELRecordSupport.registerRecordFunctions(elEval);
    elVars = new ELEvaluator.Variables();
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

  public void write(Record record) throws IOException, StageException, ELException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Path[{}] - Writing ['{}']", path, record.getHeader().getSourceId());
    }
    if (writer != null) {
      String str = recordToString.toString(record);
      Utils.checkArgument(!str.contains("\n") && !str.contains("\r"),
                          "String version of record cannot have EOL characters");
      writer.write(str);
      writer.write("\n");
    } else if (seqWriter != null) {
      ELRecordSupport.setRecordInContext(elVars, record);
      key.set((String) elEval.eval(elVars, keyEL));
      value.set(recordToString.toString(record));
      seqWriter.append(key, value);
    } else {
      throw new IOException(Utils.format("RecordWriter '{}' is closed", path));
    }
    recordCount++;
  }

  public void flush() throws IOException {
    LOG.debug("Path[{}] - Flushing", path);
    if (writer != null) {
      writer.flush();
    } else if (seqWriter != null) {
      seqWriter.hflush();
    }
  }

  // due to buffering of underlying streams, the reported length may be less than the actual one up to the
  // buffer size.
  public long getLength() throws IOException {
    long length = -1;
    if (writer != null) {
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
      if (writer != null) {
        writer.close();
      } else if (seqWriter != null) {
        seqWriter.close();
      }
    } finally {
      writer = null;
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
    return writer == null && seqWriter == null;
  }

  public String toString() {
    return Utils.format("RecordWriter[path='{}']", path);
  }

}
