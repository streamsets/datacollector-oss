/*
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.http;

import com.google.common.util.concurrent.RateLimiter;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.VaultEL;
import com.streamsets.pipeline.lib.http.AuthenticationFailureException;
import com.streamsets.pipeline.lib.http.AuthenticationType;
import com.streamsets.pipeline.lib.http.Groups;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.lib.http.JerseyClientUtil;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.origin.http.Errors;
import com.streamsets.pipeline.stage.util.http.HttpStageUtil;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.oauth1.AccessToken;
import org.glassfish.jersey.client.oauth1.OAuth1ClientSupport;
import org.glassfish.jersey.grizzly.connector.GrizzlyConnectorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.AsyncInvoker;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.streamsets.pipeline.stage.origin.http.Errors.HTTP_21;
import static com.streamsets.pipeline.stage.origin.http.Errors.HTTP_22;
import static com.streamsets.pipeline.stage.origin.http.Errors.HTTP_24;

/**
 * Processor that makes HTTP requests and stores the parsed or unparsed result in a field on a per record basis.
 * Useful for enriching records based on their content by making requests to external systems.
 */
public class HttpProcessor extends SingleLaneProcessor {
  public static final String OAUTH2_GROUP = "OAUTH2";
  public static final String CONF_CLIENT_OAUTH2_TOKEN_URL = "conf.client.oauth2.tokenUrl";

  private static final Logger LOG = LoggerFactory.getLogger(HttpProcessor.class);
  private static final String RESOURCE_CONFIG_NAME = "resourceUrl";
  private static final String REQUEST_BODY_CONFIG_NAME = "requestBody";
  private static final String HTTP_METHOD_CONFIG_NAME = "httpMethod";
  private static final String HEADER_CONFIG_NAME = "headers";
  private static final String DATA_FORMAT_CONFIG_PREFIX = "conf.dataFormatConfig.";
  private static final String SSL_CONFIG_PREFIX = "conf.sslConfig.";
  private static final String VAULT_EL_PREFIX = VaultEL.PREFIX + ":read";

  private HttpProcessorConfig conf;
  private AccessToken authToken;
  private Client client = null;
  private RateLimiter rateLimiter;

  private ELVars resourceVars;
  private ELVars bodyVars;
  private ELVars methodVars;
  private ELVars headerVars;
  private ELEval resourceEval;
  private ELEval bodyEval;
  private ELEval methodEval;
  private ELEval headerEval;

  private DataParserFactory parserFactory;
  private ErrorRecordHandler errorRecordHandler;

  private class HeadersAndBody {
    final MultivaluedMap<String, Object> resolvedHeaders;
    final String requestBody;
    final String contentType;
    final HttpMethod method;
    final WebTarget target;


    public HeadersAndBody(
        MultivaluedMap<String, Object> headers,
        String requestBody,
        String contentType,
        HttpMethod method,
        WebTarget target
    ) {
      this.resolvedHeaders = headers;
      this.requestBody = requestBody;
      this.contentType = contentType;
      this.method = method;
      this.target = target;
    }
  }

  private final Map<Record, HeadersAndBody> resolvedRecords = new HashMap<>();

  /**
   * Creates a new HttpProcessor configured using the provided config instance.
   *
   * @param conf HttpProcessor configuration
   */
  public HttpProcessor(HttpProcessorConfig conf) {
    this.conf = conf;
  }

  /** {@inheritDoc} */
  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext()); // NOSONAR

    int rateLimit = conf.rateLimit > 0 ? conf.rateLimit : Integer.MAX_VALUE;
    rateLimiter = RateLimiter.create(rateLimit);

    conf.dataFormatConfig.init(
        getContext(),
        conf.dataFormat,
        Groups.HTTP.name(),
        DATA_FORMAT_CONFIG_PREFIX,
        issues
    );

    conf.client.sslConfig.init(
        getContext(),
        Groups.SSL.name(),
        SSL_CONFIG_PREFIX,
        issues
    );

    resourceVars = getContext().createELVars();
    resourceEval = getContext().createELEval(RESOURCE_CONFIG_NAME);

    bodyVars = getContext().createELVars();
    bodyEval = getContext().createELEval(REQUEST_BODY_CONFIG_NAME);

    methodVars = getContext().createELVars();
    methodEval = getContext().createELEval(HTTP_METHOD_CONFIG_NAME);

    headerVars = getContext().createELVars();
    headerEval = getContext().createELEval(HEADER_CONFIG_NAME);

    conf.client.init(getContext(), Groups.PROXY.name(), "conf.client.", issues);
    // Validation succeeded so configure the client.
    if (issues.isEmpty()) {
      ClientConfig clientConfig = new ClientConfig()
          .property(ClientProperties.CONNECT_TIMEOUT, conf.client.connectTimeoutMillis)
          .property(ClientProperties.READ_TIMEOUT, conf.client.readTimeoutMillis)
          .property(ClientProperties.ASYNC_THREADPOOL_SIZE, conf.client.numThreads)
          .property(ClientProperties.REQUEST_ENTITY_PROCESSING, conf.client.transferEncoding)
          .connectorProvider(new GrizzlyConnectorProvider());

      ClientBuilder clientBuilder = ClientBuilder.newBuilder().withConfig(clientConfig);

      if (conf.client.useProxy) {
        JerseyClientUtil.configureProxy(conf.client.proxy, clientBuilder);
      }

      JerseyClientUtil.configureSslContext(conf.client.sslConfig, clientBuilder);

      configureAuthAndBuildClient(clientBuilder, issues);

      parserFactory = conf.dataFormatConfig.getParserFactory();
    }

    return issues;
  }

  /** {@inheritDoc} */
  @Override
  public void destroy() {
    if (client != null) {
      client.close();
    }
    super.destroy();
  }

  /** {@inheritDoc} */
  @Override
  public void process(Batch batch, SingleLaneBatchMaker batchMaker) throws StageException {
    List<Future<Response>> responses = new ArrayList<>();
    resolvedRecords.clear();

    Iterator<Record> records = batch.getRecords();
    while (records.hasNext()) {

      Record record = records.next();
      RecordEL.setRecordInContext(resourceVars, record);
      String resolvedUrl = resourceEval.eval(resourceVars, conf.resourceUrl, String.class);
      WebTarget target = client.target(resolvedUrl);

      // If the request (headers or body) contain a known sensitive EL and we're not using https then fail the request.
      if (requestContainsSensitiveInfo() && !target.getUri().getScheme().toLowerCase().startsWith("https")) {
        throw new StageException(Errors.HTTP_07);
      }

      // from HttpStreamConsumer
      final MultivaluedMap<String, Object> resolvedHeaders = resolveHeaders(record);

      String contentType = HttpStageUtil.getContentTypeWithDefault(resolvedHeaders, conf.defaultRequestContentType);

      final AsyncInvoker asyncInvoker = target.request()
          .property(OAuth1ClientSupport.OAUTH_PROPERTY_ACCESS_TOKEN, authToken)
          .headers(resolvedHeaders)
          .async();

      HttpMethod method = getHttpMethod(record);

      rateLimiter.acquire();
      if (conf.requestBody != null && !conf.requestBody.isEmpty() && method != HttpMethod.GET) {
        RecordEL.setRecordInContext(bodyVars, record);
        final String requestBody = bodyEval.eval(bodyVars, conf.requestBody, String.class);
        resolvedRecords.put(record, new HeadersAndBody(resolvedHeaders, requestBody, contentType, method, target));
        responses.add(asyncInvoker.method(method.getLabel(), Entity.entity(requestBody, contentType)));
      } else {
        resolvedRecords.put(record, new HeadersAndBody(resolvedHeaders, null, null, method, target));
        responses.add(asyncInvoker.method(method.getLabel()));
      }
    }

    records = batch.getRecords();
    int recordNum = 0;
    while (records.hasNext()) {
      try {
        Record record = processResponse(records.next(), responses.get(recordNum), conf.maxRequestCompletionSecs, false);
        if (record != null) {
          batchMaker.addRecord(record);
        }
      } catch (OnRecordErrorException e) {
        errorRecordHandler.onError(e);
      } finally {
        ++recordNum;
      }
    }
    if (!resolvedRecords.isEmpty()) {
      reprocessIfRequired(batchMaker);
    }
  }

  private void reprocessIfRequired(SingleLaneBatchMaker batchMaker) throws StageException {
    Map<Record, Future<Response>> responses = new HashMap<>(resolvedRecords.size());
    for(Map.Entry<Record, HeadersAndBody> entry : resolvedRecords.entrySet()) {
      HeadersAndBody hb = entry.getValue();
      Future<Response> responseFuture;
      final AsyncInvoker asyncInvoker = hb.target.request()
          .headers(hb.resolvedHeaders).async();
      if (hb.requestBody != null) {
        responseFuture = asyncInvoker.method(hb.method.getLabel(), Entity.entity(hb.requestBody, hb.contentType));
      } else {
        responseFuture = asyncInvoker.method(hb.method.getLabel());
      }
      responses.put(entry.getKey(), responseFuture);
    }
    for (Map.Entry<Record, Future<Response>> entry : responses.entrySet()) {
      Record output = processResponse(entry.getKey(), entry.getValue(), conf.maxRequestCompletionSecs, true);
      if (output != null) {
        batchMaker.addRecord(output);
      }
    }
  }

  /**
   * Evaluates any EL expressions in the headers section of the stage configuration.
   *
   * @param record current record in context for EL evaluation
   * @return Map of headers that can be added to the Jersey Client request
   * @throws StageException if an expression could not be evaluated
   */
  private MultivaluedMap<String, Object> resolveHeaders(Record record) throws StageException {
    RecordEL.setRecordInContext(headerVars, record);

    MultivaluedMap<String, Object> requestHeaders = new MultivaluedHashMap<>();
    for (Map.Entry<String, String> entry : conf.headers.entrySet()) {
      List<Object> header = new ArrayList<>(1);
      Object resolvedValue = headerEval.eval(headerVars, entry.getValue(), String.class);
      header.add(resolvedValue);
      requestHeaders.put(entry.getKey(), header);
    }

    return requestHeaders;
  }


  /**
   * Waits for the Jersey client to complete an asynchronous request, checks the response code
   * and continues to parse the response if it is deemed ok.
   *
   * @param record the current record to set in context for any expression evaluation
   * @param responseFuture the async HTTP request future
   * @param maxRequestCompletionSecs maximum time to wait for request completion (start to finish)
   * @return parsed record from the request
   * @throws StageException if the request fails, times out, or cannot be parsed
   */
  private Record processResponse(
      Record record,
      Future<Response> responseFuture,
      long maxRequestCompletionSecs,
      boolean failOn403
  ) throws StageException {

    Response response;
    try {
      response = responseFuture.get(maxRequestCompletionSecs, TimeUnit.SECONDS);
      String responseBody = "";
      if (response.hasEntity()) {
        responseBody = response.readEntity(String.class);
      }
      response.close();
      if (conf.client.useOAuth2 && response.getStatus() == 403 && !failOn403) {
        HttpStageUtil.getNewOAuth2Token(conf.client.oauth2, client);
        return null;
      } else if (response.getStatus() < 200 || response.getStatus() >= 300) {
        throw new OnRecordErrorException(
            record,
            Errors.HTTP_01,
            response.getStatus(),
            response.getStatusInfo().getReasonPhrase() + " " + responseBody
        );
      }
      resolvedRecords.remove(record);
      Record parsedResponse = parseResponse(responseBody);
      if (parsedResponse != null) {
        record.set(conf.outputField, parsedResponse.get());
        addResponseHeaders(record, response);
      }
      return record;
    } catch (InterruptedException | ExecutionException e) {
      LOG.error(Errors.HTTP_03.getMessage(), e.toString(), e);
      throw new OnRecordErrorException(record, Errors.HTTP_03, e.toString());
    } catch (TimeoutException e) {
      LOG.error("HTTP request future timed out", e.toString(), e);
      throw new OnRecordErrorException(record, Errors.HTTP_03, e.toString());
    }
  }


  /**
   * Parses the HTTP response text from a request into SDC Records
   *
   * @param response HTTP response
   * @return an SDC record resulting from the response text
   * @throws StageException if the response could not be parsed
   */
  private Record parseResponse(String response) throws StageException {
    Record record = null;
    try (DataParser parser = parserFactory.getParser("", response)) {
      // A response may only contain a single record, so we only parse it once.
      record = parser.parse();
      if (conf.dataFormat == DataFormat.TEXT) {
        // Output is placed in a field "/text" so we remove it here.
        record.set(record.get("/text"));
      }
    } catch (IOException | DataParserException e) {
      errorRecordHandler.onError(Errors.HTTP_00, e.toString(), e);
    }
    return record;
  }

  /**
   * Helper to apply authentication properties to Jersey client.
   *
   * @param clientBuilder Jersey Client builder to configure
   */
  private void configureAuthAndBuildClient(ClientBuilder clientBuilder, List<ConfigIssue> issues) {
    if (conf.client.authType == AuthenticationType.OAUTH) {
      authToken = JerseyClientUtil.configureOAuth1(conf.client.oauth, clientBuilder);
    } else if (conf.client.authType != AuthenticationType.NONE) {
      JerseyClientUtil.configurePasswordAuth(conf.client.authType, conf.client.basicAuth, clientBuilder);
    }
    client = clientBuilder.build();
    if (conf.client.useOAuth2) {
      try {
        conf.client.oauth2.init(getContext(), issues, client);
      } catch (AuthenticationFailureException ex) {
        LOG.error("OAuth2 Authentication failed", ex);
        issues.add(getContext().createConfigIssue(OAUTH2_GROUP, CONF_CLIENT_OAUTH2_TOKEN_URL, HTTP_21));
      } catch (IOException ex) {
        LOG.error("OAuth2 Authentication Response does not contain access token", ex);
        issues.add(getContext().createConfigIssue(OAUTH2_GROUP, CONF_CLIENT_OAUTH2_TOKEN_URL, HTTP_22));
      } catch (NotFoundException ex) {
        LOG.error(Utils.format(
            HTTP_24.getMessage(), conf.client.oauth2.tokenUrl, conf.client.oauth2.transferEncoding), ex);
        issues.add(getContext().createConfigIssue(OAUTH2_GROUP,
            CONF_CLIENT_OAUTH2_TOKEN_URL, HTTP_24, conf.client.oauth2.tokenUrl, conf.client.oauth2.transferEncoding));
      }
    }
  }

  /**
   * Determines the HTTP method to use for the next request. It may include an EL expression to evaluate.
   *
   * @param record Current record to set in context.
   * @return the {@link HttpMethod} to use for the request
   * @throws ELEvalException if an expression is supplied that cannot be evaluated
   */
  private HttpMethod getHttpMethod(Record record) throws ELEvalException {
    if (conf.httpMethod != HttpMethod.EXPRESSION) {
      return conf.httpMethod;
    }
    RecordEL.setRecordInContext(methodVars, record);
    return HttpMethod.valueOf(methodEval.eval(methodVars, conf.methodExpression, String.class));
  }

  /**
   * Returns true if the request contains potentially sensitive information such as a vault:read EL.
   *
   * @return whether or not the request had sensitive information detected.
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
   * Populates HTTP response headers to the configured location
   *
   * @param record current record to populate
   * @param response HTTP response
   * @throws StageException when writing headers to a field path that already exists
   */
  private void addResponseHeaders(Record record, Response response) throws StageException {
    if (conf.headerOutputLocation == HeaderOutputLocation.NONE) {
      return;
    }

    Record.Header header = record.getHeader();

    if (conf.headerOutputLocation == HeaderOutputLocation.FIELD) {
      writeResponseHeaderToField(record, response);
    } else if (conf.headerOutputLocation == HeaderOutputLocation.HEADER) {
      writeResponseHeaderToRecordHeader(response, header);
    }
  }

  /**
   * Writes HTTP response headers to the SDC Record at the configured field path.
   *
   * @param record Record to populate with response headers.
   * @param response HTTP response
   * @throws StageException if the field path already exists
   */
  private void writeResponseHeaderToField(Record record, Response response) throws StageException {
    if (record.has(conf.headerOutputField)) {
      throw new StageException(Errors.HTTP_11, conf.headerOutputField);
    }
    Map<String, Field> headers = new HashMap<>(response.getStringHeaders().size());

    for (Map.Entry<String, List<String>> entry : response.getStringHeaders().entrySet()) {
      if (!entry.getValue().isEmpty()) {
        String firstValue = entry.getValue().get(0);
        headers.put(entry.getKey(), Field.create(firstValue));
      }
    }

    record.set(conf.headerOutputField, Field.create(headers));
  }

  /**
   * Writes HTTP response headers to the SDC Record header with the configured optional prefix.
   *
   * @param response HTTP response
   * @param header SDC Record header
   */
  private void writeResponseHeaderToRecordHeader(Response response, Record.Header header) {
    for (Map.Entry<String, List<String>> entry : response.getStringHeaders().entrySet()) {
      if (!entry.getValue().isEmpty()) {
        String firstValue = entry.getValue().get(0);
        header.setAttribute(conf.headerAttributePrefix + entry.getKey(), firstValue);
      }
    }
  }
}
