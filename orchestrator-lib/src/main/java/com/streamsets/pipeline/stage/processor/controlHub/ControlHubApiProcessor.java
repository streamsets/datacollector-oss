/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.controlHub;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.http.Errors;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.stage.common.DataFormatErrors;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.glassfish.jersey.client.filter.CsrfProtectionFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.AsyncInvoker;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ControlHubApiProcessor extends SingleLaneProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(ControlHubApiProcessor.class);
  private static final String BASE_URL_CONFIG_NAME = "baseUrl";
  private static final String REQUEST_BODY_CONFIG_NAME = "requestBody";
  private static final String X_USER_AUTH_TOKEN = "X-SS-User-Auth-Token";
  private static final String X_SS_REST_CALL = "X-SS-REST-CALL";
  private static final String X_REQUESTED_BY = "X-Requested-By";

  private ControlHubApiConfig conf;
  private final HttpClientCommon httpClientCommon;
  private DataParserFactory parserFactory;
  private ErrorRecordHandler errorRecordHandler;

  private ELVars bodyVars;
  private ELEval bodyEval;

  private ELVars baseUrlVars;
  private ELEval baseUrlEval;

  private class HeadersAndBody {
    final MultivaluedMap<String, Object> resolvedHeaders;
    final String requestBody;
    final String contentType;
    final HttpMethod method;
    final WebTarget target;

    HeadersAndBody(
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

  private final Map<Record, HeadersAndBody> resolvedRecords = new LinkedHashMap<>();
  private final Map<String, String> resolveUrlToTokenMap = new HashMap<>();

  /**
   * Creates a new HttpProcessor configured using the provided config instance.
   *
   * @param conf HttpProcessor configuration
   */
  ControlHubApiProcessor(ControlHubApiConfig conf) {
    this.conf = conf;
    this.httpClientCommon = new HttpClientCommon(conf.client);
  }

  /** {@inheritDoc} */
  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext()); // NOSONAR

    httpClientCommon.init(issues, getContext());

    baseUrlVars = getContext().createELVars();
    baseUrlEval = getContext().createELEval(BASE_URL_CONFIG_NAME);

    bodyVars = getContext().createELVars();
    bodyEval = getContext().createELEval(REQUEST_BODY_CONFIG_NAME);

    DataParserFactoryBuilder builder = new DataParserFactoryBuilder(getContext(), DataParserFormat.JSON);
    builder.setMaxDataLen(-1).setMode(JsonMode.MULTIPLE_OBJECTS);
    try {
      parserFactory = builder.build();
    } catch (Exception ex) {
      LOG.error("Can't create parserFactory", ex);
      issues.add(getContext().createConfigIssue(null, null, DataFormatErrors.DATA_FORMAT_06, ex.toString(), ex));
    }
    return issues;
  }

  /** {@inheritDoc} */
  @Override
  public void destroy() {
    httpClientCommon.destroy();
    if(parserFactory!=null) {
      parserFactory.destroy();
    }
    super.destroy();
  }

  /** {@inheritDoc} */
  @Override
  public void process(Batch batch, SingleLaneBatchMaker batchMaker) throws StageException {
    List<Future<Response>> responses = new ArrayList<>();
    resolvedRecords.clear();
    resolveUrlToTokenMap.clear();

    Iterator<Record> records = batch.getRecords();
    while (records.hasNext()) {
      Record record = records.next();
      String resolvedUrl = getResolvedUrl(conf.baseUrl, record);
      String userAuthToken = getUserAuthToken(resolvedUrl);

      WebTarget target = httpClientCommon.getClient().target(resolvedUrl);

      LOG.debug("Resolved HTTP Client URL: '{}'",resolvedUrl);

      // from HttpStreamConsumer
      final MultivaluedMap<String, Object> resolvedHeaders = httpClientCommon.resolveHeaders(conf.headers, record);

      String contentType = "application/json";

      final AsyncInvoker asyncInvoker = target.request()
          .headers(resolvedHeaders)
          .header(X_USER_AUTH_TOKEN, userAuthToken)
          .header(X_SS_REST_CALL, "true")
          .header(X_REQUESTED_BY, "Control Hub API Processor")
          .async();

      HttpMethod method = httpClientCommon.getHttpMethod(conf.httpMethod, conf.methodExpression, record);

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
        Record record = processResponse(records.next(), responses.get(recordNum), conf.maxRequestCompletionSecs);
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
      try {
        Record output = processResponse(entry.getKey(), entry.getValue(), conf.maxRequestCompletionSecs);
        if (output != null) {
          batchMaker.addRecord(output);
        }
      } catch (OnRecordErrorException e) {
        errorRecordHandler.onError(e);
      }
    }
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
      long maxRequestCompletionSecs
  ) throws StageException {

    try (Response response = responseFuture.get(maxRequestCompletionSecs, TimeUnit.SECONDS)) {
      InputStream responseBody = null;
      if (response.hasEntity()) {
        responseBody = response.readEntity(InputStream.class);
      }
      int responseStatus = response.getStatus();
      if (responseStatus < 200 || responseStatus >= 300) {
        resolvedRecords.remove(record);
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
      } else if (responseBody == null && responseStatus != 204) {
        // Some of the control Hub APIs not returning any status
        // throw new OnRecordErrorException(record, Errors.HTTP_34);
        record.set(conf.outputField, Field.create(Field.Type.STRING, null));
      }
      return record;
    } catch (InterruptedException | ExecutionException e) {
      LOG.error(Errors.HTTP_03.getMessage(), e.toString(), e);
      throw new OnRecordErrorException(record, Errors.HTTP_03, e.toString());
    } catch (TimeoutException e) {
      LOG.error("HTTP request future timed out: {}", e.toString(), e);
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
  private Record parseResponse(InputStream response) throws StageException {
    Record record = null;
    if (response != null) {
      try (DataParser parser = parserFactory.getParser("", response, "0")) {
        // A response may only contain a single record, so we only parse it once.
        record = parser.parse();
      } catch (IOException | DataParserException e) {
        errorRecordHandler.onError(Errors.HTTP_00, e.toString(), e);
      }
    }
    return record;
  }

  private String getUserAuthToken(String resolvedUrl) throws StageException {
    // 1. Login to DPM to get user auth token
    Response response = null;
    try {
      String baseUrl;
      URL url = new URL(resolvedUrl);
      if(url.getPort() == -1){ // port is not
        baseUrl = url.getProtocol() + "://" + url.getHost() + "/";
      } else {
        baseUrl =  url.getProtocol() + "://" + url.getHost() + ":" + url.getPort() + "/";
      }

      if (!resolveUrlToTokenMap.containsKey(baseUrl)) {
        Map<String, String> loginJson = new HashMap<>();
        loginJson.put("userName", conf.client.basicAuth.username.get());
        loginJson.put("password", conf.client.basicAuth.password.get());
        response = ClientBuilder.newClient()
            .target(baseUrl + "security/public-rest/v1/authentication/login")
            .register(new CsrfProtectionFilter("CSRF"))
            .request()
            .post(Entity.json(loginJson));
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
          String reason = response.getStatusInfo().getReasonPhrase();
          String respString = response.readEntity(String.class);
          final String errorMsg = reason + " : " + respString;
          LOG.warn(Errors.HTTP_01.getMessage(), response.getStatus(), errorMsg);
        } else {
          String userAuthToken = response.getHeaderString(X_USER_AUTH_TOKEN);
          resolveUrlToTokenMap.put(baseUrl, userAuthToken);
        }
      }
      return resolveUrlToTokenMap.get(baseUrl);
    } catch (Exception e) {
      throw new StageException(ControlHubApiErrors.CONTROL_HUB_API_01, e.getMessage(), e);
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }

  public String getResolvedUrl(String baseUrl, Record record) throws ELEvalException {
    RecordEL.setRecordInContext(baseUrlVars, record);
    TimeEL.setCalendarInContext(baseUrlVars, Calendar.getInstance());
    TimeNowEL.setTimeNowInContext(baseUrlVars, new Date());

    return baseUrlEval.eval(baseUrlVars, baseUrl, String.class);
  }

}
