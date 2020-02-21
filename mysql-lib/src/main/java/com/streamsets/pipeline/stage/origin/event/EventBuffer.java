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
package com.streamsets.pipeline.stage.origin.event;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.stage.origin.mysql.error.MySQLBinLogErrors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory buffer for events collected from {@link com.github.shyiko.mysql.binlog.BinaryLogClient}.
 * <p/>
 * Events are enriched with corresponding table metadata and offset.
 */
public class EventBuffer {
  private static final Logger LOG = LoggerFactory.getLogger(EventBuffer.class);

  private final ArrayBlockingQueue<EnrichedEvent> queue;

  // signals that buffer should not accept events any more.
  private volatile boolean closed = false;

  public EventBuffer(int batchSize) {
    this.queue = new ArrayBlockingQueue<>(batchSize);
  }

  /**
   * Read next event from buffer with respect to maximum timeout.
   * @param timeout timeout.
   * @param unit timeout time unit.
   * @return next event of null
   * @throws StageException
   */
  public EnrichedEvent poll(long timeout, TimeUnit unit, boolean isPreview) throws StageException {
    try {
      return queue.poll(timeout, unit);
    } catch (InterruptedException e) {
      if (!isPreview) {
        LOG.error(MySQLBinLogErrors.MYSQL_BIN_LOG_001.getMessage(), e.toString(), e);
        Thread.currentThread().interrupt();
        throw new StageException(MySQLBinLogErrors.MYSQL_BIN_LOG_001, e.toString(), e);
      } else {
        return null;
      }
    }
  }

  public boolean put(EnrichedEvent event) {
    if (closed) {
      LOG.error("Attempt to put event to closed buffer. Rejecting event.");
      return false;
    }

    try {
      queue.put(event);
      return true;
    } catch (InterruptedException e) {
      LOG.error("Error adding event to buffer, reason: {}", e.toString(), e);
      Thread.currentThread().interrupt();
      return false;
    }
  }

  public void close() {
    closed = true;
  }
}
