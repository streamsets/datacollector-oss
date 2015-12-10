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

package com.streamsets.pipeline.stage.origin.http;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HttpClientSource extends BaseSource implements OffsetCommitter {
  private static final int SLEEP_TIME_WAITING_FOR_BATCH_SIZE_MS = 100;
  private final static Logger LOG = LoggerFactory.getLogger(HttpClientSource.class);

  private final HttpClientConfigBean conf;

  private ExecutorService executorService;
  private ScheduledExecutorService safeExecutor;

  private long recordCount;
  private DataParserFactory parserFactory;

  private BlockingQueue<String> entityQueue;
  private HttpStreamConsumer httpConsumer;

  /**
   * @param conf Configuration object for the HTTP client
   */
  public HttpClientSource(final HttpClientConfigBean conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    conf.basic.init(getContext(), issues, Groups.HTTP.name());
    conf.dataFormatConfig.init(getContext(), conf.dataFormat, Groups.HTTP.name(), issues);

    // Queue may not be empty at shutdown, but because we can't rewind,
    // the dropped entities are not recoverable anyway. In the case
    // that the pipeline is restarted we'll resume with any entities still enqueued.
    entityQueue = new ArrayBlockingQueue<>(2 * conf.basic.maxBatchSize);

    parserFactory = conf.dataFormatConfig.getParserFactory();

    httpConsumer = new HttpStreamConsumer(conf, entityQueue);

    switch (conf.httpMode) {
      case STREAMING:
        createStreamingConsumer();
        break;
      case POLLING:
        createPollingConsumer();
        break;
      default:
        throw new IllegalStateException("Unrecognized httpMode " + conf.httpMode);
    }
    return issues;
  }

  private void createPollingConsumer() {
    safeExecutor = new SafeScheduledExecutorService(1, getClass().getName());
    LOG.info("Scheduling consumer at polling period {}.", conf.pollingInterval);
    safeExecutor.scheduleAtFixedRate(httpConsumer, 0L, conf.pollingInterval, TimeUnit.MILLISECONDS);
  }

  private void createStreamingConsumer() {
    executorService = Executors.newFixedThreadPool(1);
    executorService.execute(httpConsumer);
  }

  @Override
  public void destroy() {
    if (httpConsumer != null) {
      httpConsumer.stop();
      httpConsumer = null;
      switch (conf.httpMode) {
        case STREAMING:
          executorService.shutdownNow();
          break;
        case POLLING:
          safeExecutor.shutdownNow();
          break;
      }
    }
    super.destroy();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    long start = System.currentTimeMillis();
    int chunksToFetch = Math.min(conf.basic.maxBatchSize, maxBatchSize);
    while (((System.currentTimeMillis() - start) < conf.basic.maxWaitTime) && (entityQueue.size() < chunksToFetch)) {
      try {
        Thread.sleep(SLEEP_TIME_WAITING_FOR_BATCH_SIZE_MS);
      } catch (InterruptedException ex) {
        break;
      }
    }

    List<String> chunks = new ArrayList<>(chunksToFetch);
    entityQueue.drainTo(chunks, chunksToFetch);

    Response.StatusType lastResponseStatus = httpConsumer.getLastResponseStatus();
    if (lastResponseStatus.getFamily() == Response.Status.Family.SUCCESSFUL) {
      for (String chunk : chunks) {
        String sourceId = getOffset();
        try (DataParser parser = parserFactory.getParser(sourceId, chunk.getBytes(StandardCharsets.UTF_8))) {
          if (conf.dataFormat == DataFormat.JSON) {
            // For json, a chunk only contains a single record, so we only parse it once.
            Record record = parser.parse();
            if (record != null) {
              batchMaker.addRecord(record);
              recordCount++;
            }
            if (null != parser.parse()) {
              throw new DataParserException(Errors.HTTP_02);
            }
          } else {
            // For text and xml, a chunk may contain multiple records.
            Record record = parser.parse();
            while (record != null) {
              batchMaker.addRecord(record);
              recordCount++;
              record = parser.parse();
            }
          }
        } catch (IOException | DataParserException ex) {
          switch (getContext().getOnErrorRecord()) {
            case DISCARD:
              break;
            case TO_ERROR:
              getContext().reportError(Errors.HTTP_00, sourceId, ex.toString(), ex);
              break;
            case STOP_PIPELINE:
              if (ex instanceof StageException) {
                throw (StageException) ex;
              } else {
                throw new StageException(Errors.HTTP_00, sourceId, ex.toString(), ex);
              }
            default:
              throw new IllegalStateException
                  (Utils.format("Unknown OnError value '{}'",
                  getContext().getOnErrorRecord(), ex)
              );
          }

        }
      }
    } else {
      // If http response status != 2xx
      switch (getContext().getOnErrorRecord()) {
        case DISCARD:
          break;
        case TO_ERROR:
          getContext().reportError(
              Errors.HTTP_01,
              lastResponseStatus.getStatusCode(),
              lastResponseStatus.getReasonPhrase()
          );
          break;
        case STOP_PIPELINE:
          throw new StageException(
              Errors.HTTP_01,
              lastResponseStatus.getStatusCode(),
              lastResponseStatus.getReasonPhrase()
          );
      }
    }

    return getOffset();
  }

  private String getOffset() {
    return Long.toString(recordCount);
  }

  @Override
  public void commit(String offset) throws StageException {
    // NO-OP
  }
}
