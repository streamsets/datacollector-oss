/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs.writer;


import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class ActiveRecordWriters {
  private final static Logger LOG = LoggerFactory.getLogger(ActiveRecordWriters.class);
  private final static boolean IS_TRACE_ENABLED = LOG.isTraceEnabled();

  private static class DelayedRecordWriter implements Delayed {
    private RecordWriter writer;

    public DelayedRecordWriter(RecordWriter writer) {
      this.writer = writer;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(writer.getExpiresOn() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
      long diff = writer.getExpiresOn() - ((DelayedRecordWriter)o).writer.getExpiresOn();
      return (diff > 0) ? 1 : (diff < 0) ? -1 : 0;
    }

    public RecordWriter getWriter() {
      return writer;
    }

    public String toString() {
      return Utils.format("DelayedRecordWriter[path='{}' expiresInSecs='{}'", writer.getPath(),
                          getDelay(TimeUnit.SECONDS));
    }
  }

  private RecordWriterManager manager;
  private Map<String, RecordWriter> writers;
  private DelayQueue<DelayedRecordWriter> cutOffQueue;

  public ActiveRecordWriters(RecordWriterManager manager) {
    writers = new HashMap<>();
    cutOffQueue = new DelayQueue<>();
    this.manager = manager;
  }

  public void commitOldFiles(FileSystem fs) throws IOException, ELEvalException {
    manager.commitOldFiles(fs);
  }

  public void purge() throws IOException {
    if (IS_TRACE_ENABLED) {
      LOG.trace("Purge");
    }
    DelayedRecordWriter delayedWriter = cutOffQueue.poll();
    while (delayedWriter != null) {
      if (!delayedWriter.getWriter().isClosed()) {
        if (IS_TRACE_ENABLED) {
          LOG.trace("Purging '{}'", delayedWriter.getWriter().getPath());
        }
        writers.remove(delayedWriter.getWriter().getPath().toString());
        manager.commitWriter(delayedWriter.getWriter());
      }
      delayedWriter = cutOffQueue.poll();
    }
  }

  public RecordWriter get(Date now, Date recordDate, Record record) throws StageException, IOException {
    String path = manager.getPath(recordDate, record).toString();
    RecordWriter writer = writers.get(path);
    if (writer == null) {
      writer = manager.getWriter(now, recordDate, record);
      if (writer != null) {
        if (IS_TRACE_ENABLED) {
          LOG.trace("Got '{}'", writer.getPath());
        }
        writers.put(path, writer);
        cutOffQueue.add(new DelayedRecordWriter(writer));
      }
    }
    return writer;
  }

  @VisibleForTesting
  public RecordWriterManager getWriterManager() {
    return manager;
  }

  public void release(RecordWriter writer) throws IOException {
    if (manager.isOverThresholds(writer)) {
      if (IS_TRACE_ENABLED) {
        LOG.trace("Release '{}'", writer.getPath());
      }
      writers.remove(writer.getPath().toString());
      manager.commitWriter(writer);
    }
    purge();
  }

  public void closeAll() {
    LOG.debug("Close all");
    for (RecordWriter writer : writers.values()) {
      if (!writer.isClosed()) {
        try {
          manager.commitWriter(writer);
        } catch (IOException ex) {
          String msg = Utils.format("Error closing writer {} : {}", writer, ex);
          LOG.info(msg, ex);
        }
      }
    }
    writers = null;
    cutOffQueue = null;
  }

}
