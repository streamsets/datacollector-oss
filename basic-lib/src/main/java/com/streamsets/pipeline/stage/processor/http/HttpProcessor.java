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
package com.streamsets.pipeline.stage.processor.http;

import com.google.common.util.concurrent.RateLimiter;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.http.Errors;
import com.streamsets.pipeline.lib.http.Groups;
import com.streamsets.pipeline.lib.http.HttpClientCommon;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.util.http.HttpStageUtil;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.client.oauth1.OAuth1ClientSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.AsyncInvoker;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Processor that makes HTTP requests and stores the parsed or unparsed result in a field on a per record basis.
 * Useful for enriching records based on their content by making requests to external systems.
 */
public class HttpProcessor extends SingleLaneProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(HttpProcessor.class);
  private static final String REQUEST_BODY_CONFIG_NAME = "requestBody";

  private HttpProcessorConfig conf;
  private final HttpClientCommon httpClientCommon;
  private DataParserFactory parserFactory;
  private ErrorRecordHandler errorRecordHandler;
  private RateLimiter rateLimiter;

  private ELVars bodyVars;
  private ELEval bodyEval;

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

  /**
   * Creates a new HttpProcessor configured using the provided config instance.
   *
   * @param conf HttpProcessor configuration
   */
  public HttpProcessor(HttpProcessorConfig conf) {
    this.conf = conf;
    this.httpClientCommon = new HttpClientCommon(conf.client);
  }

  /** {@inheritDoc} */
  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext()); // NOSONAR

    double rateLimit = conf.rateLimit > 0 ? (1000.0 / conf.rateLimit) : Double.MAX_VALUE;
    rateLimiter = RateLimiter.create(rateLimit);

    httpClientCommon.init(issues, getContext());

    conf.dataFormatConfig.init(
        getContext(),
        conf.dataFormat,
        Groups.HTTP.name(),
        HttpClientCommon.DATA_FORMAT_CONFIG_PREFIX,
        issues
    );

    bodyVars = getContext().createELVars();
    bodyEval = getContext().createELEval(REQUEST_BODY_CONFIG_NAME);

    if (issues.isEmpty()) {
      parserFactory = conf.dataFormatConfig.getParserFactory();
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

    Iterator<Record> records = batch.getRecords();
    while (records.hasNext()) {

      Record record = records.next();
      String resolvedUrl = httpClientCommon.getResolvedUrl(conf.resourceUrl, record);
      WebTarget target = httpClientCommon.getClient().target(resolvedUrl);

      LOG.debug("Resolved HTTP Client URL: '{}'",resolvedUrl);

      // If the request (headers or body) contain a known sensitive EL and we're not using https then fail the request.
      if (httpClientCommon.requestContainsSensitiveInfo(conf.headers, conf.requestBody) &&
          !target.getUri().getScheme().toLowerCase().startsWith("https")) {
        throw new StageException(Errors.HTTP_07);
      }

      // from HttpStreamConsumer
      final MultivaluedMap<String, Object> resolvedHeaders = httpClientCommon.resolveHeaders(conf.headers, record);

      String contentType = HttpStageUtil.getContentTypeWithDefault(resolvedHeaders, conf.defaultRequestContentType);

      final AsyncInvoker asyncInvoker = target.request()
          .property(OAuth1ClientSupport.OAUTH_PROPERTY_ACCESS_TOKEN, httpClientCommon.getAuthToken())
          .headers(resolvedHeaders)
          .async();

      HttpMethod method = httpClientCommon.getHttpMethod(conf.httpMethod, conf.methodExpression, record);

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
        Record inRec = records.next();
        List<Record> recordsResponse = processResponse(inRec, responses.get(recordNum), conf.maxRequestCompletionSecs, false);
        Response response = null;
        try {
          response = responses.get(recordNum).get(conf.maxRequestCompletionSecs, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException e) {
          LOG.error(Errors.HTTP_03.getMessage(), e.toString(), e);
          throw new OnRecordErrorException(inRec, Errors.HTTP_03, e.toString());
        } catch (TimeoutException e) {
          LOG.error("HTTP request future timed out", e.toString(), e);
          throw new OnRecordErrorException(inRec , Errors.HTTP_03, e.toString());
        }
        if (recordsResponse != null && response != null) {
          processRecord(batchMaker, recordsResponse, inRec, response);
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


  private void processRecord(SingleLaneBatchMaker batchMaker, List<Record> parsedRecords, Record inRec, Response response) throws OnRecordErrorException {
    Record firstRecord = null;
    Field field = null;
    if(parsedRecords.size()>0) {
      firstRecord = parsedRecords.get(0);
      field = inRec.get();
    }
    if (field != null) {
      try {
        final String parserId = String.format("%s_%s_%s",
            getContext().getStageInfo().getInstanceName(),
            firstRecord.getHeader().getSourceId(),
            conf.outputField
        );

        // Check the type is correct and if it's a File Ref then it is parsed using the input stream.
        switch (field.getType()) {
          case LIST_MAP:
          case LIST:
          case MAP:
          case STRING:
          case BYTE_ARRAY:
            // Do nothing...
            break;
          case FILE_REF:
            try {
              parsedRecords.clear();
              final InputStream inputStream = field.getValueAsFileRef().createInputStream(getContext(),
                  InputStream.class
              );
              byte[] fieldData = IOUtils.toByteArray(inputStream);

              try (DataParser parser = parserFactory.getParser(parserId, fieldData)) {
                parsedRecords.add(parser.parse());
              }

            } catch (IOException e) {
              throw new OnRecordErrorException(inRec,
                  Errors.HTTP_64,
                  conf.outputField,
                  inRec.getHeader().getSourceId(),
                  e.getMessage(),
                  e
              );
            }
            break;
          default:
            throw new OnRecordErrorException(inRec, Errors.HTTP_61, conf.outputField, field.getType().name());
        }

        if (parsedRecords.isEmpty()) {
          LOG.warn("No records were parsed from field {} of record {}",
              conf.outputField,
              inRec.getHeader().getSourceId()
          );
          batchMaker.addRecord(inRec);
          return;
        }
        switch (conf.multipleValuesBehavior) {
          case FIRST_ONLY:
            Map<String, Field> fieldsMap = new HashMap<>((Map<String,Field>)inRec.get().getValue());
            Field resFieldHeaders = createResponseHeaders(inRec,response);
            if(resFieldHeaders != null){
              fieldsMap.put(conf.headerOutputField.replace("/",""),resFieldHeaders);
            }
            fieldsMap.put(conf.outputField.replace("/",""),firstRecord.get());
            Record rec = getContext().cloneRecord(inRec);
            rec.set(Field.create(fieldsMap));
            batchMaker.addRecord(rec);
            break;
          case ALL_AS_LIST:
            List<Field> multipleFieldValues = new LinkedList<>();
            parsedRecords.forEach(parsedRecord -> multipleFieldValues.add(parsedRecord.get()));
            Map<String, Field> fs = new HashMap<>((Map<String,Field>)inRec.get().getValue());
            Field resField = createResponseHeaders(inRec,response);
            if(resField != null){
              fs.put(conf.headerOutputField.replace("/",""),resField);
            }
            fs.put(conf.outputField.replace("/",""),Field.create(multipleFieldValues));
            Record newRec = getContext().cloneRecord(inRec);
            newRec.set(Field.create(fs));
            batchMaker.addRecord(newRec);
            break;
          case SPLIT_INTO_MULTIPLE_RECORDS:
            int size = parsedRecords.size();
            for (int i=0; i<size; i++) {
              Record parsedRecord = parsedRecords.get(i);
              Map<String, Field> fields = new HashMap<>((Map<String,Field>)inRec.get().getValue());
              Field responseField = createResponseHeaders(inRec,response);
              if(responseField != null){
                fields.put(conf.headerOutputField.replace("/",""),responseField);
              }
              fields.put(conf.outputField.replace("/",""),parsedRecord.get());
              Record splitRecord = getContext().cloneRecord(inRec);
              splitRecord.set(Field.create(fields));
              batchMaker.addRecord(splitRecord);
            }
            break;
        }
      } catch (DataParserException ex) {
        throw new OnRecordErrorException(inRec,
            Errors.HTTP_61,
            conf.outputField,
            inRec.getHeader().getSourceId(),
            ex.toString(),
            ex
        );
      }
    } else {
      throw new OnRecordErrorException(inRec, Errors.HTTP_65, conf.outputField, inRec.getHeader().getSourceId());
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
        List<Record> output = processResponse(entry.getKey(), entry.getValue(), conf.maxRequestCompletionSecs, true);
        Response response = null;
        try {
          response = responses.get(entry.getKey()).get(conf.maxRequestCompletionSecs, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException e) {
          LOG.error(Errors.HTTP_03.getMessage(), e.toString(), e);
          throw new OnRecordErrorException(entry.getKey(), Errors.HTTP_03, e.toString());
        } catch (TimeoutException e) {
          LOG.error("HTTP request future timed out", e.toString(), e);
          throw new OnRecordErrorException(entry.getKey() , Errors.HTTP_03, e.toString());
        }
        if (output != null && response != null) {
          processRecord(batchMaker, output, entry.getKey(), response);
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
  private List<Record> processResponse(
      Record record,
      Future<Response> responseFuture,
      long maxRequestCompletionSecs,
      boolean failOn403
  ) throws StageException {

    Response response = null;
    try {
      response = responseFuture.get(maxRequestCompletionSecs, TimeUnit.SECONDS);
      InputStream responseBody = null;
      if (response.hasEntity()) {
        responseBody = response.readEntity(InputStream.class);
      }
      int responseStatus = response.getStatus();
      if (conf.client.useOAuth2 && (response.getStatus() == 403 || response.getStatus() == 401) && !failOn403) {
        HttpStageUtil.getNewOAuth2Token(conf.client.oauth2, httpClientCommon.getClient());
        return null;
      } else if (responseStatus < 200 || responseStatus >= 300) {
        resolvedRecords.remove(record);
        throw new OnRecordErrorException(
            record,
            Errors.HTTP_01,
            response.getStatus(),
            response.getStatusInfo().getReasonPhrase() + " " + responseBody
        );
      }
      resolvedRecords.remove(record);
      List<Record> parsedResponse = parseResponse(record, responseBody);
      if (conf.httpMethod != HttpMethod.HEAD && responseBody == null && responseStatus != 204) {
        throw new OnRecordErrorException(record, Errors.HTTP_34);
      }
      return parsedResponse;
    } catch (InterruptedException | ExecutionException e) {
      LOG.error(Errors.HTTP_03.getMessage(), e.toString(), e);
      throw new OnRecordErrorException(record, Errors.HTTP_03, e.toString());
    } catch (TimeoutException e) {
      LOG.error("HTTP request future timed out", e.toString(), e);
      throw new OnRecordErrorException(record, Errors.HTTP_03, e.toString());
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }


  /**
   * Parses the HTTP response text from a request into SDC Records
   *
   * @param response HTTP response
   * @return an SDC record resulting from the response text
   * @throws StageException if the response could not be parsed
   */
  private List<Record> parseResponse(Record inRecord, InputStream response) throws StageException {
    List<Record> records = new ArrayList<Record>();
    if (conf.httpMethod == HttpMethod.HEAD) {
      // Head will have no body so can't be parsed.   Return an empty record.
      Record record = getContext().createRecord("");
      record.set(Field.create(new HashMap()));
      records.add(record);
    } else if (response != null) {
      try (DataParser parser = parserFactory.getParser("", response, "0")) {
        // A response may only contain a single record, so we only parse it once.
        Record record = parser.parse();
        while(record!=null){
          if(conf.dataFormat == DataFormat.TEXT) {
            // Output is placed in a field "/text" so we remove it here.
            Record rec = getContext().cloneRecord(inRecord);
            rec.set(record.get("/text"));
            records.add(rec);
          }else if(conf.dataFormat == DataFormat.JSON || conf.dataFormat == DataFormat.XML){
            if(record.get().getValue() instanceof List) {
              // JSON Array. It returns all data in a list already. So we split each element of
              // the list in a separate record.
              ArrayList<Field> list = (ArrayList<Field>) record.get().getValue();
              for (Field f : list) {
                Record rec = getContext().cloneRecord(inRecord);
                rec.set(f);
                records.add(rec);
              }
            }else{
              //JSON Object
              records.add(record);
            }
          }else{
            records.add(record);
          }
          record = parser.parse();
        }

      } catch (IOException | DataParserException e) {
        errorRecordHandler.onError(Errors.HTTP_00, e.toString(), e);
      }
    }
    // Ensure there is always at least one record.
    if(records.size()==0){
      records.add(getContext().cloneRecord(inRecord));
    }
    return records;
  }

  /**
   * Populates HTTP response headers to the configured location
   *
   * @param record current record to populate
   * @param response HTTP response
   * @throws StageException when writing headers to a field path that already exists
   */
  private Field createResponseHeaders(Record record, Response response) throws StageException {
    if (conf.headerOutputLocation == HeaderOutputLocation.NONE) {
      return null;
    }

    Record.Header header = record.getHeader();

    if (conf.headerOutputLocation == HeaderOutputLocation.FIELD) {
      return createResponseHeaderField(record, response);
    } else if (conf.headerOutputLocation == HeaderOutputLocation.HEADER) {
      createResponseHeaderToRecordHeader(response, header);
      return null;
    }
    return null;
  }

  /**
   * Creates the HTTP response headers to the SDC Record at the configured field path.
   *
   * @param record Record to populate with response headers.
   * @param response HTTP response
   * @throws StageException if the field path already exists
   */
  private Field createResponseHeaderField(Record record, Response response) throws StageException {
    if (record.has(conf.headerOutputField) || conf.headerOutputLocation.equals(conf.outputField)) {
      throw new StageException(Errors.HTTP_11, conf.headerOutputField);
    }
    Map<String, Field> headers = new HashMap<>(response.getStringHeaders().size());

    for (Map.Entry<String, List<String>> entry : response.getStringHeaders().entrySet()) {
      if (!entry.getValue().isEmpty()) {
        String firstValue = entry.getValue().get(0);
        headers.put(entry.getKey(), Field.create(firstValue));
      }
    }

    return Field.create(headers);
  }

  /**
   * Writes HTTP response headers to the SDC Record header with the configured optional prefix.
   *
   * @param response HTTP response
   * @param header SDC Record header
   */
  private void createResponseHeaderToRecordHeader(Response response, Record.Header header) {
    for (Map.Entry<String, List<String>> entry : response.getStringHeaders().entrySet()) {
      if (!entry.getValue().isEmpty()) {
        String firstValue = entry.getValue().get(0);
        header.setAttribute(conf.headerAttributePrefix + entry.getKey(), firstValue);
      }
    }
  }
}
