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
    private final RecordWriter writer;

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

    public boolean equals(Delayed o) {
      return compareTo(o) == 0;
    }

    public RecordWriter getWriter() {
      return writer;
    }

    @Override
    public String toString() {
      return Utils.format("DelayedRecordWriter[path='{}' expiresInSecs='{}'", writer.getPath(),
                          getDelay(TimeUnit.SECONDS));
    }
  }

  private final RecordWriterManager manager;
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

  @VisibleForTesting
  public int getActiveWritersCount() {
    return cutOffQueue.size();
  }

  public void release(RecordWriter writer) throws IOException {
    if (manager.isOverThresholds(writer) || writer.isClosed()) {
      if (IS_TRACE_ENABLED) {
        LOG.trace("Release '{}'", writer.getPath());
      }
      writers.remove(writer.getPath().toString());
      manager.commitWriter(writer);
    }
    purge();
  }

  public void flushAll() {
    if (IS_TRACE_ENABLED) {
      LOG.trace("Flush all '{}'", toString());
    }
    for (RecordWriter writer : writers.values()) {
      if (!writer.isClosed()) {
        try {
          writer.flush();
        } catch (IOException ex) {
          String msg = Utils.format("Error flushing writer {} : {}", writer, ex);
          LOG.warn(msg, ex);
        }
      }
    }
  }

  public void closeAll() {
    if (IS_TRACE_ENABLED) {
      LOG.trace("Close all '{}'", toString());
    }
    if(writers != null) {
      for (RecordWriter writer : writers.values()) {
        if (!writer.isClosed()) {
          try {
            manager.commitWriter(writer);
          } catch (IOException ex) {
            String msg = Utils.format("Error closing writer {} : {}", writer, ex);
            LOG.warn(msg, ex);
          }
        }
      }
    }
    writers = null;
    cutOffQueue = null;
  }

}
