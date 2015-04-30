/*
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
  private SafeScheduledExecutorService safeExecutor;

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
  protected void init() throws StageException {
    super.init();

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
    }
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
    super.destroy();
    httpConsumer.stop();
    switch (httpMode) {
      case STREAMING:
        executorService.shutdown();
        break;
      case POLLING:
        safeExecutor.shutdown();
        break;
      default:
        throw new IllegalStateException("Unrecognized httpMode " + httpMode);
    }
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
    for (String chunk : chunks) {
      String sourceId = getOffset();
      try (DataParser parser = parserFactory.getParser(sourceId, chunk.getBytes())) {
        Record record = parser.parse();
        if (record != null) {
          batchMaker.addRecord(record);
          recordCount++;
        }
      } catch (IOException | DataParserException ex) {
        switch (getContext().getOnErrorRecord()) {
          case DISCARD:
            break;
          case TO_ERROR:
            getContext().reportError(Errors.HTTP_00, sourceId, ex.getMessage(), ex);
            break;
          case STOP_PIPELINE:
            if (ex instanceof StageException) {
              throw (StageException) ex;
            } else {
              throw new StageException(Errors.HTTP_00, sourceId, ex.getMessage(), ex);
            }
          default:
            throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
                getContext().getOnErrorRecord(), ex));
        }

      }
    }
    return getOffset();
  }

  private String getOffset() {
    return Long.toString(recordCount);
  }

  @Override
  public void commit(String offset) throws StageException {
    // NOOP
  }
}
