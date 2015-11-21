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
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
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

  private final String resourceUrl;
  private final String httpMethod;
  private final String requestData;
  private final long requestTimeoutMillis;

  /** OAuth Parameters */
  private final String consumerKey;
  private final String consumerSecret;
  private final String token;
  private final String tokenSecret;

  private final long maxBatchWaitTime;
  private final int batchSize;

  private final HttpClientMode httpMode;
  private final long pollingInterval;
  private final JsonMode jsonMode;
  private String entityDelimiter;

  private ExecutorService executorService;
  private ScheduledExecutorService safeExecutor;

  private long recordCount;
  private DataParserFactory parserFactory;

  private BlockingQueue<String> entityQueue;
  private HttpStreamConsumer httpConsumer;

  /**
   * @param config Configuration object for the HTTP client
   */
  public HttpClientSource(final HttpClientConfig config) {
    this.httpMode = config.getHttpMode();
    this.resourceUrl = config.getResourceUrl();
    this.requestTimeoutMillis = config.getRequestTimeoutMillis();
    this.entityDelimiter = config.getEntityDelimiter();
    this.batchSize = config.getBatchSize();
    this.maxBatchWaitTime = config.getMaxBatchWaitTime();
    this.consumerKey = config.getConsumerKey();
    this.consumerSecret = config.getConsumerSecret();
    this.token = config.getToken();
    this.tokenSecret = config.getTokenSecret();
    this.jsonMode = JsonMode.MULTIPLE_OBJECTS;
    this.pollingInterval = config.getPollingInterval();
    this.httpMethod = config.getHttpMethod().name();
    this.requestData = config.getRequestData();
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> errors = super.init();

    // Queue may not be empty at shutdown, but because we can't rewind,
    // the dropped entities are not recoverable anyway. In the case
    // that the pipeline is restarted we'll resume with any entities still enqueued.
    entityQueue = new ArrayBlockingQueue<>(2 * batchSize);

    parserFactory = new DataParserFactoryBuilder(getContext(), DataParserFormat.JSON)
        .setMode(jsonMode).setMaxDataLen(-1).build();

    if (token != null) {
      httpConsumer = new HttpStreamConsumer(
          resourceUrl,
          requestTimeoutMillis,
          httpMethod,
          requestData,
          entityDelimiter,
          entityQueue,
          consumerKey,
          consumerSecret,
          token,
          tokenSecret
      );
    } else {
      httpConsumer = new HttpStreamConsumer(
          resourceUrl,
          requestTimeoutMillis,
          httpMethod,
          requestData,
          entityDelimiter,
          entityQueue
      );
    }

    switch (httpMode) {
      case STREAMING:
        createStreamingConsumer();
        break;
      case POLLING:
        createPollingConsumer();
        break;
      default:
        throw new IllegalStateException("Unrecognized httpMode " + httpMode);
    }
    return errors;
  }

  private void createPollingConsumer() {
    safeExecutor = new SafeScheduledExecutorService(1, getClass().getName());
    LOG.info("Scheduling consumer at polling period {}.", pollingInterval);
    safeExecutor.scheduleAtFixedRate(httpConsumer, 0L, pollingInterval, TimeUnit.MILLISECONDS);
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
      switch (httpMode) {
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
    int chunksToFetch = Math.min(batchSize, maxBatchSize);
    while (((System.currentTimeMillis() - start) < maxBatchWaitTime) && (entityQueue.size() < chunksToFetch)) {
      try {
        Thread.sleep(SLEEP_TIME_WAITING_FOR_BATCH_SIZE_MS);
      } catch (InterruptedException ex) {
        break;
      }
    }

    List<String> chunks = new ArrayList<>(chunksToFetch);
    entityQueue.drainTo(chunks, chunksToFetch);

    if(httpConsumer.getLastResponseStatus() == 200) {
      for (String chunk : chunks) {
        String sourceId = getOffset();
        try (DataParser parser = parserFactory.getParser(sourceId, chunk.getBytes(StandardCharsets.UTF_8))) {
          Record record = parser.parse();
          // A chunk only contains a single Record, so we only parse it once.
          if (record != null) {
            batchMaker.addRecord(record);
            recordCount++;
          }
          if (null != parser.parse()) {
            throw new DataParserException(Errors.HTTP_02);
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
              throw new IllegalStateException(Utils.format("Unknown OnError value '{}'",
                getContext().getOnErrorRecord(), ex));
          }

        }
      }
    } else {
      //If http response status != 200
      switch (getContext().getOnErrorRecord()) {
        case DISCARD:
          break;
        case TO_ERROR:
          getContext().reportError(Errors.HTTP_01, chunks);
          break;
        case STOP_PIPELINE:
          throw new StageException(Errors.HTTP_01, chunks);
      }
    }

    return getOffset();
  }

  private String getOffset() {
    return Long.toString(recordCount);
  }

  @Override
  public void commit(String offset) throws StageException {
    // NOP
  }
}
