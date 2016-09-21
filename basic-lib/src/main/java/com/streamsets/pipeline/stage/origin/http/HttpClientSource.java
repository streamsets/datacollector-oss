/*
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

import com.google.common.base.Optional;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.streamsets.datacollector.el.VaultEL;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.http.AuthenticationType;
import com.streamsets.pipeline.lib.http.Groups;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.lib.http.JerseyClientUtil;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.oauth1.AccessToken;
import org.glassfish.jersey.client.oauth1.OAuth1ClientSupport;
import org.glassfish.jersey.grizzly.connector.GrizzlyConnectorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Link;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.streamsets.pipeline.lib.parser.json.Errors.JSON_PARSER_00;

/**
 * HTTP Client Origin implementation supporting streaming, polled, and paginated HTTP resources.
 */
public class HttpClientSource extends BaseSource {
  static final String START_AT = "startAt";

  private static final Logger LOG = LoggerFactory.getLogger(HttpClientSource.class);

  private static final int SLEEP_TIME_WAITING_FOR_BATCH_SIZE_MS = 100;
  private static final String RESOURCE_CONFIG_NAME = "resourceUrl";
  private static final String REQUEST_BODY_CONFIG_NAME = "requestBody";
  private static final String HEADER_CONFIG_NAME = "headers";
  private static final String DATA_FORMAT_CONFIG_PREFIX = "conf.dataFormatConfig.";
  private static final String SSL_CONFIG_PREFIX = "conf.sslConfig.";
  private static final String BASIC_CONFIG_PREFIX = "conf.basic.";
  private static final String VAULT_EL_PREFIX = VaultEL.PREFIX + ":";
  private static final HashFunction HF = Hashing.sha256();

  private final HttpClientConfigBean conf;
  private Hasher hasher;

  private AccessToken authToken;
  private Client client;
  private Response response;
  private int recordCount;
  private long lastRequestCompletedTime = -1;

  // Used for record id generation
  private String resolvedUrl;
  private String currentParameterHash;

  private ELVars resourceVars;
  private ELVars bodyVars;
  private ELVars headerVars;

  private ELEval resourceEval;
  private ELEval bodyEval;
  private ELEval headerEval;

  private DataParserFactory parserFactory;
  private ErrorRecordHandler errorRecordHandler;

  private Link next;
  private boolean haveMorePages;
  private DataParser parser = null;

  /**
   * @param conf Configuration object for the HTTP client
   */
  public HttpClientSource(final HttpClientConfigBean conf) {
    this.conf = conf;
  }

  /** {@inheritDoc} */
  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    conf.basic.init(getContext(), Groups.HTTP.name(), BASIC_CONFIG_PREFIX, issues);
    conf.dataFormatConfig.init(getContext(), conf.dataFormat, Groups.HTTP.name(), DATA_FORMAT_CONFIG_PREFIX, issues);
    conf.init(getContext(), Groups.HTTP.name(), "conf.", issues);
    conf.client.sslConfig.init(getContext(), Groups.SSL.name(), SSL_CONFIG_PREFIX, issues);

    resourceVars = getContext().createELVars();
    resourceEval = getContext().createELEval(RESOURCE_CONFIG_NAME);

    bodyVars = getContext().createELVars();
    bodyEval = getContext().createELEval(REQUEST_BODY_CONFIG_NAME);

    headerVars = getContext().createELVars();
    headerEval = getContext().createELEval(HEADER_CONFIG_NAME);

    next = null;
    haveMorePages = false;

    // Validation succeeded so configure the client.
    if (issues.isEmpty()) {
      configureClient();
    }

    return issues;
  }

  /**
   * Helper method to apply Jersey client configuration properties.
   */
  private void configureClient() {
    ClientConfig clientConfig = new ClientConfig()
        .property(ClientProperties.CONNECT_TIMEOUT, conf.client.connectTimeoutMillis)
        .property(ClientProperties.READ_TIMEOUT, conf.client.readTimeoutMillis)
        .property(ClientProperties.ASYNC_THREADPOOL_SIZE, 1)
        .property(ClientProperties.REQUEST_ENTITY_PROCESSING, conf.client.transferEncoding)
        .connectorProvider(new GrizzlyConnectorProvider());

    ClientBuilder clientBuilder = ClientBuilder.newBuilder().withConfig(clientConfig);

    configureAuth(clientBuilder);

    if (conf.client.useProxy) {
      JerseyClientUtil.configureProxy(conf.client.proxy, clientBuilder);
    }

    JerseyClientUtil.configureSslContext(conf.client.sslConfig, clientBuilder);

    client = clientBuilder.build();

    parserFactory = conf.dataFormatConfig.getParserFactory();
  }

  /**
   * Helper to apply authentication properties to Jersey client.
   *
   * @param clientBuilder Jersey Client builder to configure
   */
  private void configureAuth(ClientBuilder clientBuilder) {
    if (conf.client.authType == AuthenticationType.OAUTH) {
      authToken = JerseyClientUtil.configureOAuth1(conf.client.oauth, clientBuilder);
    } else if (conf.client.authType != AuthenticationType.NONE) {
      JerseyClientUtil.configurePasswordAuth(conf.client.authType, conf.client.basicAuth, clientBuilder);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void destroy() {
    if (response != null) {
      response.close();
      response = null;
    }
    if (client != null) {
      client.close();
      client = null;
    }
    super.destroy();
  }

  /** {@inheritDoc} */
  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    long start = System.currentTimeMillis();
    int chunksToFetch = Math.min(conf.basic.maxBatchSize, maxBatchSize);
    Optional<String> newSourceOffset = Optional.absent();
    recordCount = 0;

    setPageOffset(lastSourceOffset);

    resolvedUrl = resourceEval.eval(resourceVars, conf.resourceUrl, String.class);
    WebTarget target = client.target(resolvedUrl);

    // If the request (headers or body) contain a known sensitive EL and we're not using https then fail the request.
    if (requestContainsSensitiveInfo() && !target.getUri().getScheme().toLowerCase().startsWith("https")) {
      throw new StageException(Errors.HTTP_07);
    }

    boolean uninterrupted = true;

    while (!waitTimeExpired(start) && uninterrupted && (recordCount < chunksToFetch)) {
      if (parser != null) {
        // We already have an response that we haven't finished reading.
        newSourceOffset = Optional.of(parseResponse(start, chunksToFetch, batchMaker));
      } else if (shouldMakeRequest()) {

        if (conf.pagination.mode != PaginationMode.NONE) {
          target = client.target(resolveNextPageUrl(newSourceOffset.orNull()));
          // Pause between paging requests so we don't get rate limited.
          uninterrupted = ThreadUtil.sleep(conf.pagination.rateLimit);
        }

        makeRequest(target);
        newSourceOffset = processResponse(start, chunksToFetch, batchMaker);
      } else if (conf.httpMode == HttpClientMode.BATCH) {
        // We are done.
        return null;
      } else {
        // In polling mode, waiting for the next polling interval.
        uninterrupted = ThreadUtil.sleep(SLEEP_TIME_WAITING_FOR_BATCH_SIZE_MS);
      }
    }

    if (newSourceOffset.isPresent()) {
      return newSourceOffset.get();
    } else {
      return lastSourceOffset;
    }
  }

  /**
   * Returns the URL of the next page to fetch when paging is enabled. Otherwise
   * returns the previously configured URL.
   *
   * @param sourceOffset current source offset
   * @return next URL to fetch
   * @throws ELEvalException if the resource expression cannot be evaluated
   */
  private String resolveNextPageUrl(String sourceOffset) throws ELEvalException {
    String url;
    if (conf.pagination.mode == PaginationMode.LINK_HEADER && next != null) {
      url = next.getUri().toString();
    } else if (conf.pagination.mode == PaginationMode.BY_OFFSET || conf.pagination.mode == PaginationMode.BY_PAGE) {
      if (sourceOffset != null) {
        setPageOffset(sourceOffset);
      }
      url = resourceEval.eval(resourceVars, conf.resourceUrl, String.class);
    } else {
      url = resolvedUrl;
    }
    return url;
  }

  /**
   * Sets the startAt EL variable in scope for the resource and request body.
   * If the source offset is null (origin was reset) then the initial value
   * from the user provided configuration is used.
   *
   * @param sourceOffset source offset to parse for startAt variable.
   */
  private void setPageOffset(String sourceOffset) {
    if (conf.pagination.mode == PaginationMode.NONE) {
      return;
    }

    int startAt = conf.pagination.startAt;
    if (sourceOffset != null) {
      startAt = HttpSourceOffset.fromString(sourceOffset).getStartAt();
    }
    resourceVars.addVariable(START_AT, startAt);
    bodyVars.addVariable(START_AT, startAt);
  }

  /**
   * Returns true if the batchWaitTime has expired and we should return from produce
   *
   * @param start the time in milliseconds at which this produce call began
   * @return whether or not to return the batch as-is
   */
  private boolean waitTimeExpired(long start) {
    return (System.currentTimeMillis() - start) > conf.basic.maxWaitTime;
  }

  /**
   * Helper method to construct an HTTP request and fetch a response.
   *
   * @param target the target url to fetch.
   * @throws StageException if an unhandled error is encountered
   */
  private void makeRequest(WebTarget target) throws StageException {
    hasher = HF.newHasher();

    final Invocation.Builder invocationBuilder = target
        .request()
        .property(OAuth1ClientSupport.OAUTH_PROPERTY_ACCESS_TOKEN, authToken)
        .headers(resolveHeaders());

    if (conf.requestBody != null && !conf.requestBody.isEmpty() && conf.httpMethod != HttpMethod.GET) {
      final String requestBody = bodyEval.eval(bodyVars, conf.requestBody, String.class);
      hasher.putString(requestBody, Charset.forName(conf.dataFormatConfig.charset));
      response = invocationBuilder.method(conf.httpMethod.getLabel(), Entity.json(requestBody));
    } else {
      response = invocationBuilder.method(conf.httpMethod.getLabel());
    }

    // Calculate request parameter hash
    currentParameterHash = hasher.hash().toString();
  }

  /**
   * Determines whether or not we should continue making additional HTTP requests
   * in the current produce() call or whether to return the current batch.
   *
   * @return true if we should make additional HTTP requests for this batch
   */
  private boolean shouldMakeRequest() {
    boolean shouldMakeRequest = lastRequestCompletedTime == -1;
    shouldMakeRequest |= next != null;
    shouldMakeRequest |= (haveMorePages && conf.pagination.mode != PaginationMode.LINK_HEADER);
    shouldMakeRequest |= System.currentTimeMillis() > lastRequestCompletedTime + conf.pollingInterval &&
        conf.httpMode == HttpClientMode.POLLING;

    return shouldMakeRequest;
  }

  /**
   * Parses the response of a completed request into records and adds them to the batch.
   * If more records are available in the response than we can add to the batch, the
   * response is not closed and parsing will continue on the next batch.
   *
   * @param maxRecords maximum number of records to add to the batch.
   * @param batchMaker batch to add records to.
   * @return the next source offset to commit
   * @throws StageException if an unhandled error is encountered
   */
  private String parseResponse(long start, int maxRecords, BatchMaker batchMaker) throws StageException {
    HttpSourceOffset sourceOffset = new HttpSourceOffset(
        resolvedUrl,
        currentParameterHash,
        System.currentTimeMillis(),
        getCurrentPage()
    );
    InputStream in = null;
    if (parser == null) {
      // Only get a new parser if we are done with the old one.
      in = response.readEntity(InputStream.class);
      try {
        parser = parserFactory.getParser(sourceOffset.toString(), in, "0");
      } catch (DataParserException e) {
        if (e.getErrorCode() == JSON_PARSER_00) {
          LOG.warn("No data returned in HTTP response body.", e);
          return sourceOffset.toString();
        }
        throw e;
      }
    }
    try {
      Record record = parser.parse();
      int subRecordCount = 0;
      while (record != null && recordCount <= maxRecords && !waitTimeExpired(start)) {
        if (conf.pagination.mode != PaginationMode.NONE && record.has(conf.pagination.resultFieldPath)) {
          subRecordCount = parsePaginatedResult(batchMaker, sourceOffset.toString(), record);
          recordCount += subRecordCount;
        } else {
          addResponseHeaders(record.getHeader());
          batchMaker.addRecord(record);
          ++recordCount;
        }
        record = parser.parse();
      }

      if (record == null) {
        // Done reading this response
        cleanupResponse(in);
        incrementSourceOffset(sourceOffset, subRecordCount);
      }
    } catch (IOException e) {
      errorRecordHandler.onError(Errors.HTTP_00, e.toString(), e);
    }
    return sourceOffset.toString();
  }

  /**
   * Increments the current source offset's startAt portion by the specified amount.
   * This is the number of records parsed when paging BY_OFFSET or 1 if incrementing
   * BY_PAGE.
   *
   * @param sourceOffset the source offset
   * @param increment the amount to increment the offset by
   */
  private void incrementSourceOffset(HttpSourceOffset sourceOffset, int increment) {
    if (conf.pagination.mode == PaginationMode.BY_PAGE) {
      sourceOffset.incrementStartAt(1);
    } else if (conf.pagination.mode == PaginationMode.BY_OFFSET) {
      sourceOffset.incrementStartAt(increment);
    }
  }

  /**
   * Cleanup the {@link DataParser}, response's {@link InputStream} and the {@link Response} itself.
   * @param in the InputStream we are finished with
   * @throws IOException If a resource is not closed properly
   */
  private void cleanupResponse(InputStream in) throws IOException {
    LOG.debug("Cleanup after request processing complete.");
    parser.close();
    parser = null;
    if (in != null) {
      in.close();
    }
    lastRequestCompletedTime = System.currentTimeMillis();
    response.close();
    response = null;
  }

  /**
   * Returns the most recently requested page number or page offset requested.
   *
   * @return page number or offset
   */
  private int getCurrentPage() {
    // Body params take precedence, but usually only one or the other should be used.
    if (bodyVars.hasVariable(START_AT)) {
      return (int) bodyVars.getVariable(START_AT);
    } else if (resourceVars.hasVariable(START_AT)) {
      return (int) resourceVars.getVariable(START_AT);
    }
    return 0;
  }

  /**
   * Parses a paginated result from the configured field.
   *
   * @param batchMaker batch to add records to
   * @param sourceOffset to use as a base when creating records
   * @param record the source record containing an array to be converted to records
   * @return number of records parsed
   * @throws StageException if an unhandled error is encountered
   */
  private int parsePaginatedResult(BatchMaker batchMaker, String sourceOffset, Record record) throws
      StageException {
    int numSubRecords = 0;

    if (!record.has(conf.pagination.resultFieldPath)) {
      throw new StageException(Errors.HTTP_12, conf.pagination.resultFieldPath);
    }
    Field resultField = record.get(conf.pagination.resultFieldPath);

    if (resultField.getType() != Field.Type.LIST) {
      throw new StageException(Errors.HTTP_08, resultField.getType());
    }

    List<Field> results = resultField.getValueAsList();
    int subRecordIdx = 0;
    for (Field result : results) {
      Record r = getContext().createRecord(sourceOffset + "::" + subRecordIdx++);
      r.set(result);
      addResponseHeaders(r.getHeader());
      batchMaker.addRecord(r);
      ++numSubRecords;
    }
    haveMorePages = numSubRecords > 0;
    return numSubRecords;
  }

  /**
   * Adds the HTTP response headers to the record header.
   * @param header an SDC record header to populate
   */
  private void addResponseHeaders(Record.Header header) {
    if (response.getStringHeaders() == null) {
      return;
    }

    for (Map.Entry<String, List<String>> entry : response.getStringHeaders().entrySet()) {
      if (!entry.getValue().isEmpty()) {
        String firstValue = entry.getValue().get(0);
        header.setAttribute(entry.getKey(), firstValue);
      }
    }
  }

  /**
   * Returns true if the presence of an Vault EL function is detected.
   * @return true if the request headers or body contain a Vault EL function
   */
  private boolean requestContainsSensitiveInfo() {
    boolean sensitive = false;
    for (Map.Entry<String, String> header : conf.headers.entrySet()) {
      if (header.getKey().contains(VAULT_EL_PREFIX) || header.getValue().contains(VAULT_EL_PREFIX)) {
        sensitive = true;
        break;
      }
    }

    if (conf.requestBody != null && conf.requestBody.contains(VAULT_EL_PREFIX)) {
      sensitive = true;
    }

    return sensitive;
  }

  /**
   * Resolves any expressions in the Header value entries of the request.
   * @return map of evaluated headers to add to the request
   * @throws StageException if an unhandled error is encountered
   */
  private MultivaluedMap<String, Object> resolveHeaders() throws StageException {
    MultivaluedMap<String, Object> requestHeaders = new MultivaluedHashMap<>();
    for (Map.Entry<String, String> entry : conf.headers.entrySet()) {
      List<Object> header = new ArrayList<>(1);
      Object resolvedValue = headerEval.eval(headerVars, entry.getValue(), String.class);
      header.add(resolvedValue);
      requestHeaders.put(entry.getKey(), header);
      hasher.putString(entry.getKey(), Charset.forName(conf.dataFormatConfig.charset));
      hasher.putString(entry.getValue(), Charset.forName(conf.dataFormatConfig.charset));
    }

    return requestHeaders;
  }

  /**
   * Verifies that the response was a successful one and has data and continues to parse the response.
   * @param maxRecords maximum number of records to add to the batch
   * @param batchMaker batch of records to populate
   * @return a new source offset if the response was successful
   * @throws StageException if an unhandled error is encountered
   */
  private Optional<String> processResponse(long start, int maxRecords, BatchMaker batchMaker) throws
      StageException {
    Optional<String> newSourceOffset = Optional.absent();

    // Response was not in the OK range, so treat as an error
    int status = response.getStatus();
    if (status < 200 || status >= 300) {
      lastRequestCompletedTime = System.currentTimeMillis();
      String reason = response.getStatusInfo().getReasonPhrase();
      String respString = response.readEntity(String.class);
      response.close();
      response = null;

      errorRecordHandler.onError(Errors.HTTP_01, status, reason + " : " + respString);

      return newSourceOffset;
    }

    if (conf.pagination.mode == PaginationMode.LINK_HEADER) {
      next = response.getLink("next");
      if (next == null) {
        haveMorePages = false;
      }
    }


    if (response.hasEntity()) {
      newSourceOffset = Optional.of(parseResponse(start, maxRecords, batchMaker));
    }

    return newSourceOffset;
  }
}
