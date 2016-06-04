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
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.lib.http.Groups;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
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
  private static final Logger LOG = LoggerFactory.getLogger(HttpClientSource.class);
  private static final int SLEEP_TIME_WAITING_FOR_BATCH_SIZE_MS = 100;
  private static final String DATA_FORMAT_CONFIG_PREFIX = "conf.dataFormatConfig.";
  private static final String SSL_CONFIG_PREFIX = "conf.sslConfig.";

  public static final String BASIC_CONFIG_PREFIX = "conf.basic.";

  private final HttpClientConfigBean conf;

  private ExecutorService executorService;
  private ScheduledExecutorService safeExecutor;

  private long recordCount;
  private DataParserFactory parserFactory;
  private ErrorRecordHandler errorRecordHandler;

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
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    conf.basic.init(getContext(), Groups.HTTP.name(), BASIC_CONFIG_PREFIX, issues);
    conf.dataFormatConfig.init(getContext(), conf.dataFormat, Groups.HTTP.name(), DATA_FORMAT_CONFIG_PREFIX, issues);
    conf.init(getContext(), Groups.HTTP.name(), "conf.", issues);
    conf.sslConfig.init(getContext(), Groups.SSL.name(), "conf.sslConfig.", issues);

    // Queue may not be empty at shutdown, but because we can't rewind,
    // the dropped entities are not recoverable anyway. In the case
    // that the pipeline is restarted we'll resume with any entities still enqueued.
    entityQueue = new ArrayBlockingQueue<>(2 * conf.basic.maxBatchSize);

    parserFactory = conf.dataFormatConfig.getParserFactory();

    httpConsumer = new HttpStreamConsumer(conf, getContext(), entityQueue);

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
      if (conf.httpMode == HttpClientMode.STREAMING) {
        executorService.shutdownNow();
      } else if (conf.httpMode == HttpClientMode.POLLING) {
        safeExecutor.shutdownNow();
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
        Thread.currentThread().interrupt();
      }
    }

    // Check for an error and propagate to the user
    if (httpConsumer.getError().isPresent()) {
      Exception e = httpConsumer.getError().get();
      if (e instanceof StageException) {
        throw (StageException) e;
      } else {
        throw new StageException(Errors.HTTP_03, e.getMessage(), e);
      }
    }

    List<String> chunks = new ArrayList<>(chunksToFetch);

    // We didn't receive any new data within the time allotted for this batch.
    if (entityQueue.isEmpty()) {
      return getOffset();
    }
    entityQueue.drainTo(chunks, chunksToFetch);

    Response.StatusType lastResponseStatus = httpConsumer.getLastResponseStatus();
    if (lastResponseStatus.getFamily() == Response.Status.Family.SUCCESSFUL) {
      for (String chunk : chunks) {
        String sourceId = getOffset();
        try (DataParser parser = parserFactory.getParser(sourceId, chunk.getBytes(StandardCharsets.UTF_8))) {
          parseChunk(parser, batchMaker);
        } catch (IOException | DataParserException e) {
          errorRecordHandler.onError(Errors.HTTP_00, sourceId, e.toString(), e);
        }
      }
    } else {
      // If http response status != 2xx
      errorRecordHandler.onError(Errors.HTTP_01, lastResponseStatus.getStatusCode(), lastResponseStatus.getReasonPhrase());
    }

    return getOffset();
  }

  private void parseChunk(DataParser parser, BatchMaker batchMaker) throws IOException, DataParserException {
    if (conf.dataFormat == DataFormat.JSON) {
      // For JSON, a chunk only contains a single record, so we only parse it once.
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
  }

  private String getOffset() {
    return Long.toString(recordCount);
  }

  @Override
  public void commit(String offset) throws StageException {
    // NO-OP
  }
}
