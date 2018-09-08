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
package com.streamsets.pipeline.stage.destination.datalake.writer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.stage.destination.datalake.IdleClosedException;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import org.apache.commons.io.output.CountingOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

final class DataLakeDataGenerator {
  private final static Logger LOG = LoggerFactory.getLogger(DataLakeDataGenerator.class);

  private Future<Void> currentIdleCloseFuture = null;
  private volatile boolean idleClosed;

  private final String filePath;
  private final OutputStreamHelper outputStreamHelper;
  private final DataGenerator generator;
  private final CountingOutputStream cos;
  private final ReadWriteLock closeLock = new ReentrantReadWriteLock();
  private final long idleTimeSecs;
  private final AtomicLong recordCount;

  private ScheduledThreadPoolExecutor idleCloseExecutor = new ScheduledThreadPoolExecutor(
      1,
      new ThreadFactoryBuilder().setNameFormat("Data Lake Idle Close Thread").build()
  );


  DataLakeDataGenerator(
      String filePath,
      OutputStreamHelper outputStreamHelper,
      DataGeneratorFormatConfig dataFormatConfig,
      long idleTimeSecs
  ) throws IOException, StageException {
    this.filePath = filePath;
    this.outputStreamHelper = outputStreamHelper;
    this.cos = new CountingOutputStream(outputStreamHelper.getOutputStream(filePath));
    this.generator = dataFormatConfig.getDataGeneratorFactory().getGenerator(
        cos,
        outputStreamHelper.getStreamCloseEventHandler()
    );
    this.idleTimeSecs = idleTimeSecs;
    this.recordCount = new AtomicLong(0L);
    this.idleClosed = false;
  }

  String getFilePath() {
    return filePath;
  }

  long getRecordCount() {
    return this.recordCount.get();
  }

  long getByteCount() {
    return this.cos.getByteCount();
  }

  void close() throws IOException, StageException {
    close(false);
  }

  void write(Record record) throws IOException, StageException {
    closeLock.writeLock().lock();
    try {
      generator.write(record);
      recordCount.incrementAndGet();
    } finally {
      closeLock.writeLock().unlock();
    }
  }

  void flush() throws IOException {
    scheduleIdleClose();
    throwIfIdleClosed();

    closeLock.writeLock().lock();
    try {
      if (generator != null) {
        generator.flush();
      }
    } finally {
      closeLock.writeLock().unlock();
    }
  }

  // Private Methods
  private void close(boolean idleClosed) throws IOException, StageException {
    closeLock.writeLock().lock();
    LOG.debug("Path[{}] - Closing", filePath);
    try {
      throwIfIdleClosed();
      // If this was closed previously, just return
      if (isClosed()) {
        return;
      }

      if (generator != null) {
        generator.close();
      }

      this.idleClosed = idleClosed;
      // writers can never be null, except in tests
      if (idleClosed && outputStreamHelper != null) {
        //writers.release(this, false);
        outputStreamHelper.commitFile(filePath);
      }
    } finally {
      closeLock.writeLock().unlock();
      //Gracefully Shutdown the thread, so rename goes through without glitch.
      if (!idleClosed) {
        idleCloseExecutor.shutdown();
      }
    }
  }


  private boolean isClosed() {
    closeLock.readLock().lock();
    boolean isClosed = (generator == null);
    closeLock.readLock().unlock();
    return isClosed;
  }

  private void scheduleIdleClose() {
    if (idleTimeSecs > 0) {
      closeLock.writeLock().lock();
      try {
        if (currentIdleCloseFuture != null && !currentIdleCloseFuture.isDone()) {
          currentIdleCloseFuture.cancel(false);
        }
        if (idleCloseExecutor != null && !idleCloseExecutor.isShutdown()) {
          currentIdleCloseFuture = idleCloseExecutor.schedule(new IdleCloseCallable(), idleTimeSecs, TimeUnit.SECONDS);
        }
      } catch (Exception ex) {
        LOG.warn(Utils.format("Error while attempting to schedule idle closing for path {}", filePath));
      } finally {
        closeLock.writeLock().unlock();
      }
    }
  }

  private void throwIfIdleClosed() throws IdleClosedException {
    if (idleClosed) {
      throw new IdleClosedException(Utils.format("{} was closed because the file was idle for {} seconds.",
          filePath,
          idleTimeSecs
      ));
    }
  }

  private class IdleCloseCallable implements Callable<Void> {
    @Override
    public Void call() throws StageException{
      try {
        if (generator != null) {
          close(true);
        }
      } catch (IOException e) {
        LOG.error("Error while attempting to close " + filePath, e);
      }
      return null;
    }
  }

}
