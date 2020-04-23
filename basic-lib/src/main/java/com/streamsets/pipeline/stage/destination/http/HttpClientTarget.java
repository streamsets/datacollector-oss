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
package com.streamsets.pipeline.stage.destination.http;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.http.Errors;
import com.streamsets.pipeline.lib.http.Groups;
import com.streamsets.pipeline.lib.http.HttpClientCommon;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.destination.lib.ResponseType;
import com.streamsets.pipeline.stage.origin.restservice.RestServiceReceiver;
import com.streamsets.pipeline.stage.util.http.HttpStageUtil;
import org.glassfish.jersey.client.oauth1.OAuth1ClientSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.AsyncInvoker;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class HttpClientTarget extends BaseTarget {

  private static final Logger LOG = LoggerFactory.getLogger(HttpClientTarget.class);
  private static final String REQUEST_STATUS_CONFIG_NAME = "HTTP-Status";
  private final HttpClientTargetConfig conf;
  private final HttpClientCommon httpClientCommon;
  private DataGeneratorFactory generatorFactory;
  private ErrorRecordHandler errorRecordHandler;
  private RateLimiter rateLimiter;

  protected HttpClientTarget(HttpClientTargetConfig conf) {
    this.conf = conf;
    this.httpClientCommon = new HttpClientCommon(conf.client);
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    int rateLimit = conf.rateLimit > 0 ? conf.rateLimit : Integer.MAX_VALUE;
    rateLimiter = RateLimiter.create(rateLimit);
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    this.httpClientCommon.init(issues, getContext());
    if(issues.size() == 0) {
      conf.dataGeneratorFormatConfig.init(
          getContext(),
          conf.dataFormat,
          Groups.HTTP.name(),
          HttpClientCommon.DATA_FORMAT_CONFIG_PREFIX,
          issues
      );
      generatorFactory = conf.dataGeneratorFormatConfig.getDataGeneratorFactory();
    }
    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    if (conf.singleRequestPerBatch) {
      writeOneRequestPerBatch(batch);
    } else {
      writeOneRequestPerRecord(batch);
    }
  }

  private void writeOneRequestPerBatch(Batch batch) throws StageException {
    Response response = null;
    try {
      if (batch.getRecords().hasNext()) {
        // Use first record for resolving url, headers, ...
        Record firstRecord = batch.getRecords().next();
        MultivaluedMap<String, Object> resolvedHeaders =  httpClientCommon.resolveHeaders(conf.headers, firstRecord);
        Invocation.Builder builder = getBuilder(firstRecord).headers(resolvedHeaders);
        String contentType = HttpStageUtil.getContentTypeWithDefault(resolvedHeaders, getContentType());
        HttpMethod method = httpClientCommon.getHttpMethod(conf.httpMethod, conf.methodExpression, firstRecord);

        if (method == HttpMethod.POST || method == HttpMethod.PUT || method == HttpMethod.PATCH ||
            method == HttpMethod.DELETE) {
          StreamingOutput streamingOutput = outputStream -> {
            try (DataGenerator dataGenerator = generatorFactory.getGenerator(outputStream)) {
              Iterator<Record> records = batch.getRecords();
              while (records.hasNext()) {
                Record record = records.next();
                dataGenerator.write(record);
              }
              dataGenerator.flush();
            } catch (DataGeneratorException e) {
              throw new IOException(e);
            }
          };
          response = builder.method(method.getLabel(), Entity.entity(streamingOutput, contentType));
        } else {
          response = builder.method(method.getLabel());
        }

        String responseBody = "";
        if (response.hasEntity()) {
          responseBody = response.readEntity(String.class);
        }
        if (conf.client.useOAuth2 && (response.getStatus() == 403 || response.getStatus() == 401)) {
          HttpStageUtil.getNewOAuth2Token(conf.client.oauth2, httpClientCommon.getClient());
        } else if (response.getStatus() < 200 || response.getStatus() >= 300) {
          errorRecordHandler.onError(
              Lists.newArrayList(batch.getRecords()),
              new OnRecordErrorException(
                  Errors.HTTP_40,
                  response.getStatus(),
                  response.getStatusInfo().getReasonPhrase() + " " + responseBody
              )
          );
        } else {
          // Success case
          if (conf.responseConf.sendResponseToOrigin) {
            if (ResponseType.SUCCESS_RECORDS.equals(conf.responseConf.responseType)) {
              Iterator<Record> records = batch.getRecords();
              while (records.hasNext()) {
                getContext().toSourceResponse(records.next());
              }
            } else {
              getContext().toSourceResponse(createResponseRecord(responseBody,response.getStatus()));
            }
          }
        }
      }
    } catch (Exception ex) {
      LOG.error(Errors.HTTP_41.getMessage(), ex.toString(), ex);
      errorRecordHandler.onError(Lists.newArrayList(batch.getRecords()), new StageException(Errors.HTTP_41, ex, ex));
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }

  private void writeOneRequestPerRecord(Batch batch) throws StageException {
    List<Future<Response>> responses = new ArrayList<>();
    Iterator<Record> records = batch.getRecords();
    while (records.hasNext()) {
      Record record = records.next();
      MultivaluedMap<String, Object> resolvedHeaders =  httpClientCommon.resolveHeaders(conf.headers, record);
      AsyncInvoker asyncInvoker = getBuilder(record).headers(resolvedHeaders).async();
      String contentType = HttpStageUtil.getContentTypeWithDefault(resolvedHeaders, getContentType());
      HttpMethod method = httpClientCommon.getHttpMethod(conf.httpMethod, conf.methodExpression, record);
      rateLimiter.acquire();
      try {
        if (method == HttpMethod.POST || method == HttpMethod.PUT || method == HttpMethod.PATCH) {
          StreamingOutput streamingOutput = outputStream -> {
            try (DataGenerator dataGenerator = generatorFactory.getGenerator(outputStream)) {
              dataGenerator.write(record);
              dataGenerator.flush();
            } catch (DataGeneratorException e) {
              throw new IOException(e);
            }
          };
          responses.add(asyncInvoker.method(method.getLabel(), Entity.entity(streamingOutput, contentType)));
        } else {
          responses.add(asyncInvoker.method(method.getLabel()));
        }
      } catch (Exception ex) {
        LOG.error(Errors.HTTP_41.getMessage(), ex.toString(), ex);
        throw new OnRecordErrorException(record, Errors.HTTP_41, ex.toString());
      }
    }

    records = batch.getRecords();
    int recordNum = 0;
    while (records.hasNext()) {
      try {
        processResponse(records.next(), responses.get(recordNum), conf.maxRequestCompletionSecs, false);
      } catch (OnRecordErrorException e) {
        errorRecordHandler.onError(e);
      } finally {
        ++recordNum;
      }
    }
  }

  private Invocation.Builder getBuilder(Record record) throws StageException {
    String resolvedUrl = httpClientCommon.getResolvedUrl(conf.resourceUrl, record);
    WebTarget target = httpClientCommon.getClient().target(resolvedUrl);
    // If the request (headers or body) contain a known sensitive EL and we're not using https then fail the request
    if (httpClientCommon.requestContainsSensitiveInfo(conf.headers, null) &&
        !target.getUri().getScheme().toLowerCase().startsWith("https")) {
      throw new StageException(Errors.HTTP_07);
    }
    return target.request()
        .property(OAuth1ClientSupport.OAUTH_PROPERTY_ACCESS_TOKEN, httpClientCommon.getAuthToken());
  }

  /**
   * Waits for the Jersey client to complete an asynchronous request, checks the response code
   * and continues to parse the response if it is deemed ok.
   *
   * @param record the current record to set in context for any expression evaluation
   * @param responseFuture the async HTTP request future
   * @param maxRequestCompletionSecs maximum time to wait for request completion (start to finish)
   * @throws StageException if the request fails, times out, or cannot be parsed
   */
  private void processResponse(
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
        HttpStageUtil.getNewOAuth2Token(conf.client.oauth2, httpClientCommon.getClient());
      } else if (response.getStatus() < 200 || response.getStatus() >= 300) {
        throw new OnRecordErrorException(
            record,
            Errors.HTTP_40,
            response.getStatus(),
            response.getStatusInfo().getReasonPhrase() + " " + responseBody
        );
      } else {
        if (conf.responseConf.sendResponseToOrigin) {
          if (ResponseType.SUCCESS_RECORDS.equals(conf.responseConf.responseType)) {
            getContext().toSourceResponse(record);
          } else {
            getContext().toSourceResponse(createResponseRecord(responseBody,response.getStatus()));
          }
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      LOG.error(Errors.HTTP_41.getMessage(), e.toString(), e);
      throw new OnRecordErrorException(record, Errors.HTTP_41, e.toString());
    } catch (TimeoutException e) {
      LOG.error("HTTP request future timed out", e.toString(), e);
      throw new OnRecordErrorException(record, Errors.HTTP_41, e.toString());
    }
  }

  private String getContentType() {
    switch (conf.dataFormat) {
      case TEXT:
        return MediaType.TEXT_PLAIN;
      case BINARY:
        return MediaType.APPLICATION_OCTET_STREAM;
      case JSON:
      case SDC_JSON:
        return MediaType.APPLICATION_JSON;
      default:
        // Default is binary blob
        return MediaType.APPLICATION_OCTET_STREAM;
    }
  }

  private Record createResponseRecord(String responseBody, int status) {
    Record responseRecord = getContext().createRecord("responseRecord");
    responseRecord.set(Field.create(responseBody));
    responseRecord.getHeader().setAttribute(RestServiceReceiver.RAW_DATA_RECORD_HEADER_ATTR_NAME, "true");
    responseRecord.getHeader().setAttribute(REQUEST_STATUS_CONFIG_NAME,String.format("%d",status));
    return responseRecord;
  }

  @Override
  public void destroy() {
    this.httpClientCommon.destroy();
    super.destroy();
  }
}
