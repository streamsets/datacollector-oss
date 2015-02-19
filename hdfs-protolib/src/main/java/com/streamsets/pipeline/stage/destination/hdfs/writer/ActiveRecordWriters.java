/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs.writer;


import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
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

  public void purge() throws IOException {
    LOG.debug("Purge");
    DelayedRecordWriter delayedWriter = cutOffQueue.poll();
    while (delayedWriter != null) {
      if (!delayedWriter.getWriter().isClosed()) {
        LOG.debug("Purging '{}'", delayedWriter.getWriter().getPath());
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
        LOG.debug("Got '{}'", writer.getPath());
        writers.put(path, writer);
        cutOffQueue.add(new DelayedRecordWriter(writer));
      }
    }
    return writer;
  }

  public void release(RecordWriter writer) throws IOException {
    if (manager.isOverThresholds(writer)) {
      LOG.debug("Release '{}'", writer.getPath());
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
          //TODO LOG
        }
      }
    }
    writers = null;
    cutOffQueue = null;
  }

}
