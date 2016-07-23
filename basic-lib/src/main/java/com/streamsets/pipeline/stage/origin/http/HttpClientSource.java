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

import com.google.common.base.Optional;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.streamsets.datacollector.el.VaultEL;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.config.DataFormat;
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
import org.glassfish.jersey.client.ChunkedInput;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.oauth1.AccessToken;
import org.glassfish.jersey.client.oauth1.OAuth1ClientSupport;
import org.glassfish.jersey.grizzly.connector.GrizzlyConnectorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.AsyncInvoker;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class HttpClientSource extends BaseSource {
  private static final Logger LOG = LoggerFactory.getLogger(HttpClientSource.class);
  private static final int SLEEP_TIME_WAITING_FOR_BATCH_SIZE_MS = 100;
  private static final String RESOURCE_CONFIG_NAME = "resourceUrl";
  private static final String REQUEST_BODY_CONFIG_NAME = "requestBody";
  private static final String HEADER_CONFIG_NAME = "headers";
  private static final String DATA_FORMAT_CONFIG_PREFIX = "conf.dataFormatConfig.";
  private static final String SSL_CONFIG_PREFIX = "conf.sslConfig.";
  private static final String BASIC_CONFIG_PREFIX = "conf.basic.";
  private static final String VAULT_EL_PREFIX = "${" + VaultEL.PREFIX;

  private final HttpClientConfigBean conf;
  private static final HashFunction hf = Hashing.sha256();
  private Hasher hasher;

  private AccessToken authToken;
  private Client client;
  private Response response;
  private ChunkedInput<String> chunkedInput;
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
    conf.client.sslConfig.init(getContext(), Groups.SSL.name(), SSL_CONFIG_PREFIX, issues);

    resourceVars = getContext().createELVars();
    resourceEval = getContext().createELEval(RESOURCE_CONFIG_NAME);

    bodyVars = getContext().createELVars();
    bodyEval = getContext().createELEval(REQUEST_BODY_CONFIG_NAME);

    headerVars = getContext().createELVars();
    headerEval = getContext().createELEval(HEADER_CONFIG_NAME);

    // Validation succeeded so configure the client.
    if (issues.isEmpty()) {
      configureClient();
    }

    return issues;
  }

  private void configureClient() {
    ClientConfig clientConfig = new ClientConfig()
        .property(ClientProperties.ASYNC_THREADPOOL_SIZE, 1)
        .property(ClientProperties.READ_TIMEOUT, conf.client.requestTimeoutMillis)
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

  @Override
  public void destroy() {
    if (chunkedInput != null && !chunkedInput.isClosed()) {
      chunkedInput.close();
      chunkedInput = null;
    }
    if (response != null) {
      response.close();
      response = null;
    }
    super.destroy();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    long start = System.currentTimeMillis();
    int chunksToFetch = Math.min(conf.basic.maxBatchSize, maxBatchSize);
    Optional<String> newSourceOffset = Optional.absent();
    recordCount = 0;

    resolvedUrl = resourceEval.eval(resourceVars, conf.resourceUrl, String.class);
    WebTarget target = client.target(resolvedUrl);

    // If the request (headers or body) contain a known sensitive EL and we're not using https then fail the request.
    if (requestContainsSensitiveInfo() && !target.getUri().getScheme().toLowerCase().startsWith("https")) {
      throw new StageException(Errors.HTTP_07);
    }

    Future<Response> responseFuture;
    while (((System.currentTimeMillis() - start) < conf.basic.maxWaitTime) && (recordCount < chunksToFetch)) {
      if (shouldMakeRequest(start)) {
        responseFuture = makeRequest(target);
        newSourceOffset = processResponse(responseFuture, chunksToFetch, batchMaker);
      } else if (chunkedInput != null) {
        // We already have a chunkedInput that we haven't finished reading.
        newSourceOffset = readChunks(chunksToFetch, batchMaker);
      } else {
        // In polling mode, waiting for the next polling interval.
        if (!ThreadUtil.sleep(SLEEP_TIME_WAITING_FOR_BATCH_SIZE_MS)) {
          break;
        }
      }
    }

    if (newSourceOffset.isPresent()) {
      return newSourceOffset.get();
    } else {
      return lastSourceOffset;
    }
  }

  private Future<Response> makeRequest(WebTarget target) throws StageException {
    Future<Response> responseFuture;
    hasher = hf.newHasher();

    final AsyncInvoker asyncInvoker = target
        .request()
        .property(OAuth1ClientSupport.OAUTH_PROPERTY_ACCESS_TOKEN, authToken)
        .headers(resolveHeaders())
        .async();

    if (conf.requestBody != null && !conf.requestBody.isEmpty() && conf.httpMethod != HttpMethod.GET) {
      final String requestBody = bodyEval.eval(bodyVars, conf.requestBody, String.class);
      hasher.putString(requestBody, Charset.forName(conf.dataFormatConfig.charset));
      responseFuture = asyncInvoker.method(conf.httpMethod.getLabel(), Entity.json(requestBody));
    } else {
      responseFuture = asyncInvoker.method(conf.httpMethod.getLabel());
    }

    // Calculate request parameter hash
    currentParameterHash = hasher.hash().toString();
    return responseFuture;
  }

  private boolean shouldMakeRequest(long start) {
    return chunkedInput == null && start > lastRequestCompletedTime + conf.pollingInterval;
  }

  private String parseChunk(String chunk, BatchMaker batchMaker) throws IOException, DataParserException {
    // Paging support will be added in SDC-3352
    String newSourceOffset = "url::" + resolvedUrl + "::page::0::params::" + currentParameterHash + "::time::" +
        System.currentTimeMillis();
    DataParser parser = parserFactory.getParser(newSourceOffset, chunk);
    if (conf.dataFormat == DataFormat.JSON) {
      // For JSON, a chunk only contains a single record, so we only parse it once.
      Record record = parser.parse();
      if (record != null) {
        batchMaker.addRecord(record);
        ++recordCount;
      }
      if (null != parser.parse()) {
        throw new DataParserException(Errors.HTTP_02);
      }
    } else {
      // For text and xml, a chunk may contain multiple records.
      Record record = parser.parse();
      while (record != null) {
        batchMaker.addRecord(record);
        ++recordCount;
        record = parser.parse();
      }
    }
    LOG.debug("Read record with ID, count: '{}', '{}'", newSourceOffset, recordCount);
    return newSourceOffset;
  }

  private void configureAuth(ClientBuilder clientBuilder) {
    if (conf.client.authType == AuthenticationType.OAUTH) {
      authToken = JerseyClientUtil.configureOAuth1(conf.client.oauth, clientBuilder);
    } else if (conf.client.authType != AuthenticationType.NONE) {
      JerseyClientUtil.configurePasswordAuth(conf.client.authType, conf.client.basicAuth, clientBuilder);
    }
  }

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

  private Optional<String> processResponse(Future<Response> responseFuture, int maxRecords, BatchMaker batchMaker) throws
      StageException {
    Optional<String> newSourceOffset = Optional.absent();
    try {
      response = responseFuture.get(conf.client.requestTimeoutMillis, TimeUnit.MILLISECONDS);

      // Response was not in the OK range, so treat as an error
      if (response.getStatus() < 200 || response.getStatus() >= 300) {
        errorRecordHandler.onError(
            Errors.HTTP_01,
            response.getStatus(),
            response.getStatusInfo().getReasonPhrase()
        );
        lastRequestCompletedTime = System.currentTimeMillis();
        response.close();
        response = null;
        return newSourceOffset;
      }

      if (response.hasEntity()) {
        chunkedInput = response.readEntity(new GenericType<ChunkedInput<String>>() {
        });
        chunkedInput.setParser(ChunkedInput.createParser(conf.entityDelimiter));
        newSourceOffset = readChunks(maxRecords, batchMaker);
      }
    } catch (InterruptedException | TimeoutException | ExecutionException e) {
      LOG.error(Errors.HTTP_03.getMessage(), e.toString(), e);
      errorRecordHandler.onError(Errors.HTTP_03, e.toString());
    }

    return newSourceOffset;
  }

  private Optional<String> readChunks(int maxRecords, BatchMaker batchMaker) throws StageException {
    String chunk = null;
    Optional<String> newSourceOffset = Optional.absent();
    try {
      while (recordCount < maxRecords && (chunk = chunkedInput.read()) != null) {
        newSourceOffset = Optional.of(parseChunk(chunk, batchMaker));
      }
    } catch (IOException e) {
      errorRecordHandler.onError(Errors.HTTP_00, chunk, e.toString(), e);
    } finally {
      // This was the end of the chunked input
      // If there's more to read we can't close the response in this batch.
      if (chunk == null) {
        chunkedInput.close();
        chunkedInput = null;
        response.close();
        response = null;
        lastRequestCompletedTime = System.currentTimeMillis();
      }
    }
    return newSourceOffset;
  }
}
