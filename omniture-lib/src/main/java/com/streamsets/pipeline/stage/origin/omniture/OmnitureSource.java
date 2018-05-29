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
package com.streamsets.pipeline.stage.origin.omniture;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.ext.DataCollectorServices;
import com.streamsets.pipeline.api.ext.json.JsonMapper;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class OmnitureSource extends BaseSource {
  private static final int SLEEP_TIME_WAITING_FOR_BATCH_SIZE_MS = 100;
  private final static Logger LOG = LoggerFactory.getLogger(OmnitureSource.class);

  private final String resourceUrl;
  private final String reportDescription;
  private final long requestTimeoutMillis;

  /** WSSE Parameters */
  private final CredentialValue username;
  private final CredentialValue sharedSecret;

  private final long maxBatchWaitTime;
  private final int batchSize;

  private final HttpClientMode httpMode;
  private final long pollingInterval;
  private final HttpProxyConfigBean proxySettings;
  private final JsonMapper jsonMapper;

  private ScheduledExecutorService executorService;

  private long recordCount;
  private DataParserFactory parserFactory;
  private ErrorRecordHandler errorRecordHandler;

  private BlockingQueue<String> entityQueue;
  private OmniturePollingConsumer httpConsumer;

  /**
   * Constructor for authenticated clients. Requires only the resource URL.
   * @param config Configuration for Omniture API requests
   */
  public OmnitureSource(final OmnitureConfig config) {
    this.httpMode = config.getHttpMode();
    this.resourceUrl = config.getResourceUrl();
    this.requestTimeoutMillis = config.getRequestTimeoutMillis();
    this.batchSize = config.getBatchSize();
    this.maxBatchWaitTime = config.getMaxBatchWaitTime();
    this.pollingInterval = config.getPollingInterval();
    this.username = config.getUsername();
    this.sharedSecret = config.getSharedSecret();
    this.reportDescription = config.getReportDescription();
    this.proxySettings = config.getProxySettings();
    this.jsonMapper = DataCollectorServices.instance().get(JsonMapper.SERVICE_KEY);
  }

  /**
   * Validate Ominture Report Description.
   */
  private void validateReportDescription(List<ConfigIssue> issues){
    if(!jsonMapper.isValidJson(this.reportDescription)) {
      issues.add(
          getContext().createConfigIssue(
              Groups.REPORT.name(),
              "reportDescription",
              Errors.OMNITURE_03
          ));
    }
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> errors = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    // TODO: SDC-552 - Omniture origin should be recoverable
    entityQueue = new ArrayBlockingQueue<>(2 * batchSize);

    parserFactory = new DataParserFactoryBuilder(getContext(), DataParserFormat.JSON)
        .setMode(JsonMode.MULTIPLE_OBJECTS).setMaxDataLen(-1).build();

    boolean useProxy = proxySettings != null;
    String proxyUsername = null;
    String proxyPassword = null;
    String proxyUri = null;
    if(useProxy) {
      proxyUsername = proxySettings.resolveUsername(getContext(), "PROXY", "proxySettings.", errors);
      proxyPassword = proxySettings.resolvePassword(getContext(), "PROXY", "proxySettings.", errors);
      proxyUri = proxySettings.proxyUri;
    }

    switch (httpMode) {
      case POLLING:
        validateReportDescription(errors);
        httpConsumer = new OmniturePollingConsumer(
            resourceUrl,
            reportDescription,
            requestTimeoutMillis,
            username,
            sharedSecret,
            entityQueue,
            useProxy,
            proxyUri,
            proxyUsername,
            proxyPassword
        );
        createPollingConsumer();
        break;
      default:
        throw new IllegalStateException("Unrecognized httpMode " + httpMode);
    }
    return errors;
  }

  private void createPollingConsumer() {
    executorService = new SafeScheduledExecutorService(1, getClass().getName());
    LOG.info("Scheduling consumer at polling period {}.", pollingInterval);
    executorService.scheduleAtFixedRate(httpConsumer, 0L, pollingInterval, TimeUnit.MILLISECONDS);
  }

  @Override
  public void destroy() {
    super.destroy();
    switch (httpMode) {
      case POLLING:
        if (httpConsumer != null) {
          httpConsumer.stop();
        }
        if (executorService != null) {
          executorService.shutdownNow();
        }
        break;
      default:
        throw new IllegalStateException("Unrecognized httpMode " + httpMode);
    }
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    long start = System.currentTimeMillis();
    int chunksToFetch = Math.min(batchSize, maxBatchSize);
    while ((((System.currentTimeMillis() - start) < maxBatchWaitTime))
        && (entityQueue.size() < chunksToFetch)) {
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
      try (DataParser parser = parserFactory.getParser(sourceId, chunk.getBytes(StandardCharsets.UTF_8))) {
        Record record = parser.parse();
        if (record != null) {
          batchMaker.addRecord(record);
          recordCount++;
        }
      } catch (IOException | DataParserException ex) {
        errorRecordHandler.onError(Errors.OMNITURE_00, sourceId, ex.toString(), ex);
      }
    }
    return getOffset();
  }

  private String getOffset() {
    return Long.toString(recordCount);
  }
}
