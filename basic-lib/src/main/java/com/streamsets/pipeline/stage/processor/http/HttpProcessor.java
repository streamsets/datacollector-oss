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


import com.amazonaws.util.IOUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
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
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.http.Errors;
import com.streamsets.pipeline.lib.http.Groups;
import com.streamsets.pipeline.lib.http.HttpClientCommon;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.common.MultipleValuesBehavior;
import com.streamsets.pipeline.stage.origin.http.HttpResponseActionConfigBean;
import com.streamsets.pipeline.stage.origin.http.PaginationMode;
import com.streamsets.pipeline.stage.origin.http.ResponseAction;
import com.streamsets.pipeline.stage.util.http.HttpStageUtil;
import org.apache.commons.lang.StringUtils;
import org.glassfish.jersey.client.oauth1.OAuth1ClientSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.AsyncInvoker;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Link;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.streamsets.pipeline.lib.http.Errors.HTTP_66;

/**
 * Processor that makes HTTP requests and stores the parsed or unparsed result in a field on a per record basis.
 * Useful for enriching records based on their content by making requests to external systems.
 */
public class HttpProcessor extends SingleLaneProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(HttpProcessor.class);
  private static final String REQUEST_BODY_CONFIG_NAME = "requestBody";
  private static final String RESOURCE_CONFIG_NAME = "resourceUrl";
  private static final String REQUEST_STATUS_CONFIG_NAME = "HTTP-Status";
  private static final String STOP_CONFIG_NAME = "stopCondition";
  private static final String START_AT = "startAt";

  private static final Set<PaginationMode> LINK_PAGINATION = ImmutableSet.of(PaginationMode.LINK_HEADER,
      PaginationMode.LINK_FIELD
  );

  private Link next;
  private String resolvedUrl;
  private final HttpProcessorConfig conf;
  private final HttpClientCommon httpClientCommon;
  private DataParserFactory parserFactory;
  private ErrorRecordHandler errorRecordHandler;
  private RateLimiter rateLimiter;
  private Response response;
  private boolean lastRequestTimedOut = false;
  private boolean haveMorePages;
  private boolean appliedRetryAction;
  private boolean renewedToken;

  private ELVars resourceVars;
  private ELVars bodyVars;
  private ELEval bodyEval;
  private ELEval resourceEval;
  private ELEval stopEval;
  private ELVars stopVars;

  private final Map<Record, HeadersAndBody> resolvedRecords = new LinkedHashMap<>();

  private static class ResponseState {
    private long backoffIntervalLinear = 0;
    private long backoffIntervalExponential = 0;
    private int retryCount = 0;
    private int lastStatus = 0;
    private boolean lastRequestTimedOut;
  }

  private final Map<Record, ResponseState> recordsToResponseState = new HashMap<>();

  private final Map<Integer, HttpResponseActionConfigBean> statusToActionConfigs = new HashMap<>();
  private HttpResponseActionConfigBean timeoutActionConfig;

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
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    double rateLimit = conf.rateLimit > 0 ? (1000.0 / conf.rateLimit) : Double.MAX_VALUE;
    rateLimiter = RateLimiter.create(rateLimit);

    httpClientCommon.init(issues, getContext());

    conf.dataFormatConfig.init(getContext(),
        conf.dataFormat,
        Groups.HTTP.name(),
        HttpClientCommon.DATA_FORMAT_CONFIG_PREFIX,
        issues
    );

    bodyVars = getContext().createELVars();
    bodyEval = getContext().createELEval(REQUEST_BODY_CONFIG_NAME);

    resourceVars = getContext().createELVars();
    resourceEval = getContext().createELEval(RESOURCE_CONFIG_NAME);

    stopVars = getContext().createELVars();
    stopEval = getContext().createELEval(STOP_CONFIG_NAME);

    next = null;
    haveMorePages = false;

    HttpStageUtil.validateStatusActionConfigs(issues,
        getContext(),
        conf.responseStatusActionConfigs,
        statusToActionConfigs,
        "conf.responseStatusActionConfigs"
    );

    this.timeoutActionConfig = conf.responseTimeoutActionConfig;

    if (issues.isEmpty()) {
      parserFactory = conf.dataFormatConfig.getParserFactory();
    }

    return issues;
  }

  @Override
  public void destroy() {
    httpClientCommon.destroy();
    if (parserFactory != null) {
      parserFactory.destroy();
    }
    super.destroy();
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
   * Sets the startAt EL variable in scope for the resource and request body.
   * If the source offset is null (origin was reset) then the initial value
   * from the user provided configuration is used.
   */
  private void initPageOffset() {
    switch (conf.pagination.mode) {
      case LINK_HEADER:
      case LINK_FIELD:
      case BY_OFFSET:
      case BY_PAGE:
        int startAt = conf.pagination.startAt;
        resourceVars.addVariable(START_AT, startAt);
        bodyVars.addVariable(START_AT, startAt);
        break;
      default:
    }
  }

  @VisibleForTesting
  String resolveInitialUrl(Record record) {
    RecordEL.setRecordInContext(resourceVars, record);
    TimeEL.setCalendarInContext(resourceVars, Calendar.getInstance());
    TimeNowEL.setTimeNowInContext(resourceVars, new Date());
    return resourceEval.eval(resourceVars, conf.resourceUrl, String.class);
  }

  /**
   * Helper method to construct an HTTP request and fetch a response.
   *
   * @param target the target url to fetch.
   */
  private Future<Response> makeRequest(WebTarget target, Record record) {
    MultivaluedMap<String, Object> resolvedHeaders = httpClientCommon.resolveHeaders(conf.headers, record);
    String contentType = HttpStageUtil.getContentTypeWithDefault(resolvedHeaders, conf.defaultRequestContentType);
    final AsyncInvoker asyncInvoker = target.request()
                                            .property(OAuth1ClientSupport.OAUTH_PROPERTY_ACCESS_TOKEN,
                                                httpClientCommon.getAuthToken()
                                            )
                                            .headers(resolvedHeaders)
                                            .async();

    HttpMethod method = httpClientCommon.getHttpMethod(conf.httpMethod, conf.methodExpression, record);
    Future<Response> futureResp = null;

    long startTime = System.currentTimeMillis();


    try {
      rateLimiter.acquire();
      if (conf.requestBody != null && !conf.requestBody.isEmpty() && method != HttpMethod.GET) {
        RecordEL.setRecordInContext(bodyVars, record);
        final String requestBody = bodyEval.eval(bodyVars, conf.requestBody, String.class);
        resolvedRecords.put(record, new HeadersAndBody(resolvedHeaders, requestBody, contentType, method, target));
        futureResp = asyncInvoker.method(method.getLabel(), Entity.entity(requestBody, contentType));

      } else {
        resolvedRecords.put(record, new HeadersAndBody(resolvedHeaders, null, null, method, target));
        futureResp = asyncInvoker.method(method.getLabel());
      }
      LOG.debug("Retrieved response in {} ms", System.currentTimeMillis() - startTime);
      lastRequestTimedOut = false;
    } catch (Exception e) {
      LOG.debug("Request failed after {} ms", System.currentTimeMillis() - startTime);
      final Throwable cause = e.getCause();
      if (cause != null && (cause instanceof TimeoutException || cause instanceof SocketTimeoutException)) {
        LOG.warn("{} attempting to read response in HttpClientSource: {}",
            cause.getClass().getSimpleName(),
            e.getMessage(),
            e
        );

        lastRequestTimedOut = true;

      } else if (cause instanceof InterruptedException) {
        LOG.error(String.format("InterruptedException attempting to make request in HttpClientSource; stopping: %s",
            e.getMessage()
            ),
            e
        );

      } else {
        LOG.error(String.format("ProcessingException attempting to make request in HttpClientSource: %s",
            e.getMessage()
        ), e);
        Throwable reportEx = cause != null ? cause : e;
        final StageException stageException = new StageException(Errors.HTTP_32,
            getResponseStatus(),
            reportEx.toString(),
            reportEx
        );
        LOG.error(stageException.getMessage());
        throw stageException;
      }
    }
    return futureResp;
  }


  @VisibleForTesting
  int getCurrentPage() {
    // Body params take precedence, but usually only one or the other should be used.
    if (bodyVars.hasVariable(START_AT)) {
      return (int) bodyVars.getVariable(START_AT);
    } else if (resourceVars.hasVariable(START_AT)) {
      return (int) resourceVars.getVariable(START_AT);
    }
    return 0;
  }

  @Override
  public void process(Batch batch, SingleLaneBatchMaker batchMaker) {
    resolvedRecords.clear();
    long start = System.currentTimeMillis();
    Iterator<Record> records = batch.getRecords();
    boolean close = false;
    while (records.hasNext()) {

      boolean uninterrupted = true;
      Record record = records.next();
      int countPages = conf.pagination.startAt;
      initPageOffset();

      String initialResolvedURL = resolveInitialUrl(record);
      WebTarget target = httpClientCommon.getClient().target(initialResolvedURL);

      LOG.debug("Resolved HTTP Client URL: '{}'", initialResolvedURL);

      // If the request (headers or body) contain a known sensitive EL and we're not using https then fail the request.
      if (httpClientCommon.requestContainsSensitiveInfo(conf.headers, conf.requestBody) &&
          !target.getUri().getScheme().toLowerCase().startsWith("https")) {
        throw new StageException(Errors.HTTP_07);
      }

      List<Record> addToBatchRecords;
      List<Record> recordsResponse;

      Record currentRecord = null;

      do {
        recordsResponse = new ArrayList<>();
        Future<Response> future = makeRequest(target, record);
        int numRecordsLastRequest;

        try {
          close = processResponse(record, future, conf.maxRequestCompletionSecs, false, recordsResponse, start);

          completeRequest(record, future);

          if (conf.pagination.mode != PaginationMode.NONE && !appliedRetryAction && !renewedToken) {
            Record recordResp = recordsResponse.get(0);
            String resultFieldPath = conf.pagination.keepAllFields ? conf.pagination.resultFieldPath : "";
            List listResponse = (List) recordResp.get(resultFieldPath).getValue();
            numRecordsLastRequest = listResponse.size();
            if (currentRecord == null) {
              currentRecord = recordResp;
            } else {
              ((List) currentRecord.get(resultFieldPath).getValue()).addAll(listResponse);
            }
            if (conf.pagination.mode == PaginationMode.BY_PAGE) {
              countPages++;
            } else {
              countPages = countPages + numRecordsLastRequest;
            }
            String s = resolveNextPageUrl(countPages + "");
            if (s == null) {
              close = true;
            } else {
              target = httpClientCommon.getClient().target(s);
            }
            // Pause between paging requests so we don't get rate limited.
            uninterrupted = ThreadUtil.sleep(conf.pagination.rateLimit);
          } else if (!renewedToken) {
            currentRecord = getContext().createRecord("");
            currentRecord.set(Field.create(recordsResponse.stream().map(Record::get).collect(Collectors.toList())));
          }
        } catch (OnRecordErrorException e) {
          close = true;
          errorRecordHandler.onError(e);
        }
      } while (shouldMakeAnotherRequest(start, uninterrupted, close, recordsToResponseState.get(record)));

      if (recordsResponse != null && response != null) {
        addToBatchRecords = processRecord(currentRecord, record, response);
        for (Record recToAdd : addToBatchRecords) {
          batchMaker.addRecord(recToAdd);
        }
      }
    }

    if (!resolvedRecords.isEmpty() && !close) {
      reprocessIfRequired(batchMaker);
    }
  }

  private void completeRequest(Record record, Future<Response> future) {
    try {
      future.get(conf.maxRequestCompletionSecs, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException e) {
      LOG.error(Errors.HTTP_03.getMessage(), e.toString(), e);
      throw new OnRecordErrorException(record, Errors.HTTP_03, e.toString());
    } catch (TimeoutException e) {
      LOG.error("HTTP request future timed out", e.toString(), e);
      throw new OnRecordErrorException(record, Errors.HTTP_03, e.toString());
    }
  }

  private boolean shouldMakeAnotherRequest(
      long start, boolean uninterrupted, boolean close, ResponseState responseState
  ) {
    boolean waitTimeNotExp = !waitTimeExpired(start);
    boolean thereIsNextLink = (
        ((conf.pagination.mode == PaginationMode.LINK_FIELD) || (conf.pagination.mode == PaginationMode.LINK_HEADER)) &&
            haveMorePages
    );
    boolean isLink = !(
        conf.pagination.mode == PaginationMode.BY_PAGE ||
            conf.pagination.mode == PaginationMode.BY_OFFSET ||
            conf.pagination.mode == PaginationMode.NONE
    );

    HttpResponseActionConfigBean action = statusToActionConfigs.get(responseState.lastStatus);
    boolean numRetriesExceed = action != null && (responseState.retryCount > action.getMaxNumRetries());
    return (waitTimeNotExp &&
        uninterrupted &&
        !lastRequestTimedOut &&
        !close &&
        conf.multipleValuesBehavior != MultipleValuesBehavior.FIRST_ONLY &&
        (thereIsNextLink || !isLink || appliedRetryAction) &&
        !numRetriesExceed)
        || renewedToken;
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
    if (StringUtils.isNotEmpty(sourceOffset) && StringUtils.isNumeric(sourceOffset)) {
      startAt = Integer.parseInt(sourceOffset);
    }

    resourceVars.addVariable(START_AT, startAt);
    bodyVars.addVariable(START_AT, startAt);
  }

  @VisibleForTesting
  String getResolvedUrl() {
    return resolvedUrl;
  }

  @VisibleForTesting
  void setResolvedUrl(String resolvedUrl) {
    this.resolvedUrl = resolvedUrl;
  }


  /**
   * Returns the URL of the next page to fetch when paging is enabled. Otherwise
   * returns the previously configured URL.
   *
   * @param sourceOffset current source offset
   * @return next URL to fetch
   */
  @VisibleForTesting
  String resolveNextPageUrl(String sourceOffset) {
    String url;
    if (LINK_PAGINATION.contains(conf.pagination.mode) && next != null) {
      url = next.getUri().toString();
      setResolvedUrl(url);
    } else if (conf.pagination.mode == PaginationMode.BY_OFFSET || conf.pagination.mode == PaginationMode.BY_PAGE) {
      if (sourceOffset != null) {
        setPageOffset(sourceOffset);
      }
      url = resourceEval.eval(resourceVars, conf.resourceUrl, String.class);
    } else {
      url = getResolvedUrl();
    }
    return url;
  }


  private List<Record> processRecord(Record currentRecord, Record inRec, Response response) {
    List<Record> recordsProcessed = new ArrayList<>();

    if (currentRecord == null) {
      return recordsProcessed;
    }

    Record firstRecord = currentRecord;
    Field field = inRec.get();

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
            currentRecord = parseFileRefRecord(inRec, response, field, parserId);
            break;
          default:
            throw new OnRecordErrorException(inRec,
                Errors.HTTP_61,
                getResponseStatus(response),
                conf.outputField,
                field.getType().name()
            );
        }

        Object currentRecordValue = currentRecord.get().getValue();
        List<Field> currentRecordList;
        if (currentRecordValue instanceof List) {
          currentRecordList = (List<Field>) currentRecordValue;
        } else {
          currentRecordList = Collections.singletonList(currentRecord.get());
        }

        if (currentRecordList.isEmpty()) {
          // No results
          switch (conf.missingValuesBehavior) {
            case SEND_TO_ERROR:
              LOG.error(Errors.HTTP_68.getMessage(), getResponseStatus(response));
              Record cloneRecord = getContext().cloneRecord(inRec);
              errorRecordHandler.onError(new OnRecordErrorException(cloneRecord,
                  Errors.HTTP_68,
                  getResponseStatus(response)
              ));
              break;
            case PASS_RECORD_ON:
              Record recToAddToBatch = getContext().cloneRecord(inRec);
              recordsProcessed.add(recToAddToBatch);
              break;
            default:
              throw new IllegalStateException("Unknown missing value behavior: " + conf.missingValuesBehavior);
          }
        } else {
          switch (conf.multipleValuesBehavior) {
            case FIRST_ONLY:
              Map<String, Field> fieldsMap = new HashMap<>((Map<String, Field>) inRec.get().getValue());
              Field resFieldHeaders = createResponseHeaders(inRec, response);
              if (resFieldHeaders != null) {
                fieldsMap.put(conf.headerOutputField.replace("/", ""), resFieldHeaders);
              }
              fieldsMap.put(conf.outputField.replace("/", ""), currentRecordList.get(0));
              Record recToAddToBatch = getContext().cloneRecord(inRec);
              recToAddToBatch.set(Field.create(fieldsMap));
              recordsProcessed.add(recToAddToBatch);
              break;
            case ALL_AS_LIST:
              Map<String, Field> fs = new HashMap<>((Map<String, Field>) inRec.get().getValue());
              Field resField = createResponseHeaders(inRec, response);
              if (resField != null) {
                fs.put(conf.headerOutputField.replace("/", ""), resField);
              }
              fs.put(conf.outputField.replace("/", ""), Field.create(currentRecordList));
              Record recToAddToBatchList = getContext().cloneRecord(inRec);
              recToAddToBatchList.set(Field.create(fs));
              recordsProcessed.add(recToAddToBatchList);
              break;
            case SPLIT_INTO_MULTIPLE_RECORDS:
              for (Field value : currentRecordList) {
                Record parsedRecord = getContext().createRecord("");
                parsedRecord.set(value);
                Map<String, Field> fields = new HashMap<>((Map<String, Field>) inRec.get().getValue());
                Field responseField = createResponseHeaders(inRec, response);
                if (responseField != null) {
                  fields.put(conf.headerOutputField.replace("/", ""), responseField);
                }
                fields.put(conf.outputField.replace("/", ""), parsedRecord.get());
                Record recToAddToBatchSplit = getContext().cloneRecord(inRec);
                recToAddToBatchSplit.set(Field.create(fields));
                recordsProcessed.add(recToAddToBatchSplit);
              }
              break;
          }
        }
      } catch (DataParserException ex) {
        throw new OnRecordErrorException(inRec,
            Errors.HTTP_61,
            getResponseStatus(response),
            conf.outputField,
            inRec.getHeader().getSourceId(),
            ex.toString(),
            ex
        );
      }
    } else {
      throw new OnRecordErrorException(inRec,
          Errors.HTTP_65,
          getResponseStatus(response),
          conf.outputField,
          inRec.getHeader().getSourceId()
      );
    }

    return recordsProcessed;
  }

  private Record parseFileRefRecord(Record inRec, Response response, Field field, String parserId) {
    Record currentRecord;
    try {
      final InputStream inputStream = field.getValueAsFileRef().createInputStream(getContext(),
          InputStream.class
      );
      byte[] fieldData = IOUtils.toByteArray(inputStream);

      try (DataParser parser = parserFactory.getParser(parserId, fieldData)) {
        currentRecord = parser.parse();
      }

    } catch (IOException e) {
      throw new OnRecordErrorException(
          inRec,
          Errors.HTTP_64,
          getResponseStatus(response),
          conf.outputField,
          inRec.getHeader().getSourceId(),
          e.getMessage(),
          e
      );
    }
    return currentRecord;
  }

  private void reprocessIfRequired(SingleLaneBatchMaker batchMaker) {
    Map<Record, Future<Response>> responses = new HashMap<>(resolvedRecords.size());
    for (Map.Entry<Record, HeadersAndBody> entry : resolvedRecords.entrySet()) {
      HeadersAndBody hb = entry.getValue();
      Future<Response> responseFuture;
      final AsyncInvoker asyncInvoker = hb.target.request().headers(hb.resolvedHeaders).async();
      if (hb.requestBody != null) {
        responseFuture = asyncInvoker.method(hb.method.getLabel(), Entity.entity(hb.requestBody, hb.contentType));
      } else {
        responseFuture = asyncInvoker.method(hb.method.getLabel());
      }
      responses.put(entry.getKey(), responseFuture);
    }

    for (Map.Entry<Record, Future<Response>> entry : responses.entrySet()) {
      try {
        List<Record> recordsToAddBatch;
        List<Record> output = new ArrayList<>();
        processResponse(entry.getKey(),
            entry.getValue(),
            conf.maxRequestCompletionSecs,
            true,
            output,
            System.currentTimeMillis()
        );
        Response reprocessResponse = reprocessResponse(entry, responses);

        if (reprocessResponse != null) {
          recordsToAddBatch = processRecord(output.get(0), entry.getKey(), reprocessResponse);
          for (Record record : recordsToAddBatch) {
            batchMaker.addRecord(record);
          }
        }
      } catch (OnRecordErrorException e) {
        errorRecordHandler.onError(e);
      }
    }
  }

  private Response reprocessResponse(
      Map.Entry<Record, Future<Response>> entry, Map<Record, Future<Response>> responses
  ) {
    try {
      return responses.get(entry.getKey()).get(conf.maxRequestCompletionSecs, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException e) {
      LOG.error(Errors.HTTP_03.getMessage(), getResponseStatus(), e.toString(), e);
      throw new OnRecordErrorException(entry.getKey(), Errors.HTTP_03, getResponseStatus(), e.toString());
    } catch (TimeoutException e) {
      LOG.error("HTTP request future timed out: {}", e.toString(), e);
      throw new OnRecordErrorException(entry.getKey(), Errors.HTTP_03, getResponseStatus(), e.toString());
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
   */
  private boolean processResponse(
      Record record,
      Future<Response> responseFuture,
      long maxRequestCompletionSecs,
      boolean failOn403,
      List<Record> records,
      long start
  ) {
    appliedRetryAction = false;
    renewedToken = false;
    ResponseState responseState;

    if (recordsToResponseState.containsKey(record)) {
      responseState = recordsToResponseState.get(record);
    } else {
      responseState = new ResponseState();
    }

    Response recordResponse = null;
    try {
      recordResponse = responseFuture.get(maxRequestCompletionSecs, TimeUnit.SECONDS);
      setResponse(recordResponse);

      InputStream responseBody = null;
      if (recordResponse.hasEntity()) {
        responseBody = recordResponse.readEntity(InputStream.class);
      }
      final HttpResponseActionConfigBean action = statusToActionConfigs.get(recordResponse.getStatus());
      int responseStatus = recordResponse.getStatus();
      if (conf.client.useOAuth2 &&
          (recordResponse.getStatus() == 403 || recordResponse.getStatus() == 401) &&
          !failOn403) {
        HttpStageUtil.getNewOAuth2Token(conf.client.oauth2, httpClientCommon.getClient());
        renewedToken = true;
        return false;
      } else if (responseStatus < 200 || responseStatus >= 300) {
        resolvedRecords.remove(record);
        if (action == null) {
          if (conf.propagateAllHttpResponses) {
            Map<String, Field> mapFields = new HashMap<>();
            String responseBodyString = extractResponseBodyStr(recordResponse);
            mapFields.put(conf.errorResponseField, Field.create(responseBodyString));
            Record r = getContext().createRecord("");
            r.set(Field.create(mapFields));
            createResponseHeaders(r, recordResponse);
            r.getHeader().setAttribute(REQUEST_STATUS_CONFIG_NAME, String.format("%d", getResponse().getStatus()));
            records.add(r);
            return false;
          } else {
            throw new OnRecordErrorException(record,
                Errors.HTTP_01,
                recordResponse.getStatus(),
                recordResponse.getStatusInfo().getReasonPhrase() + " " + responseBody
            );
          }

        } else {
          final boolean statusChanged = responseState.lastStatus != responseStatus || responseState.lastRequestTimedOut;

          final AtomicInteger retryCountObj = new AtomicInteger(responseState.retryCount);
          final AtomicLong backoffExp = new AtomicLong(responseState.backoffIntervalExponential);
          final AtomicLong backoffLin = new AtomicLong(responseState.backoffIntervalLinear);
          String responseBodyString = extractResponseBodyStr(recordResponse);
          HttpStageUtil.applyResponseAction(action,
              statusChanged,
              input -> new StageException(Errors.HTTP_14, responseStatus, responseBodyString),
              retryCountObj,
              backoffLin,
              backoffExp,
              record,
              String.format("action defined for status %d (response: %s)", responseStatus, responseBodyString)
          );
          responseState.retryCount = retryCountObj.get();
          responseState.backoffIntervalExponential = backoffExp.get();
          responseState.backoffIntervalLinear = backoffLin.get();
          responseState.lastRequestTimedOut = true;
          appliedRetryAction = action.getAction() == ResponseAction.RETRY_EXPONENTIAL_BACKOFF ||
              action.getAction() == ResponseAction.RETRY_LINEAR_BACKOFF ||
              action.getAction() == ResponseAction.RETRY_IMMEDIATELY;
        }
      }
      resolvedRecords.remove(record);
      boolean close = false;

      if (action != null) {
        if (waitTimeExpired(start)) {
          close = true;
          errorRecordHandler.onError(Errors.HTTP_67, getResponseStatus());
        }
      } else {
        close = parseResponse(record, responseBody, records);
      }

      if (conf.httpMethod != HttpMethod.HEAD && responseBody == null && responseStatus != 204) {
        throw new OnRecordErrorException(record, Errors.HTTP_34, responseStatus);
      } else if (responseStatus == 204 && records.isEmpty()) {
        Record recordEmpty = getContext().cloneRecord(record);
        records.add(recordEmpty);
      }

      responseState.lastRequestTimedOut = false;
      responseState.lastStatus = responseStatus;
      return close;
    } catch (InterruptedException | ExecutionException e) {
      LOG.error(Errors.HTTP_03.getMessage(), getResponseStatus(), e.toString(), e);
      throw new OnRecordErrorException(record, Errors.HTTP_03, getResponseStatus(), e.toString());
    } catch (TimeoutException e) {
      final HttpResponseActionConfigBean actionConf = this.timeoutActionConfig;

      final boolean firstTimeout = !responseState.lastRequestTimedOut;

      final AtomicInteger retryCountObj = new AtomicInteger(responseState.retryCount);
      final AtomicLong backoffExp = new AtomicLong(responseState.backoffIntervalExponential);
      final AtomicLong backoffLin = new AtomicLong(responseState.backoffIntervalLinear);
      HttpStageUtil.applyResponseAction(actionConf,
          firstTimeout,
          input -> new StageException(Errors.HTTP_18),
          retryCountObj,
          backoffLin,
          backoffExp,
          record,
          "action defined for timeout"
      );
      responseState.retryCount = retryCountObj.get();
      responseState.backoffIntervalExponential = backoffExp.get();
      responseState.backoffIntervalLinear = backoffLin.get();
      responseState.lastRequestTimedOut = true;
      return false;

    } finally {
      recordsToResponseState.put(record, responseState);
      if (recordResponse != null) {
        recordResponse.close();
      }
    }
  }

  private String extractResponseBodyStr(Response response) {
    String bodyStr = "";
    if (response != null) {
      try {
        bodyStr = response.readEntity(String.class);
      } catch (ProcessingException e) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("IOException attempting to read response stream to String: {}", e.getMessage(), e);
        }
      }
    }
    return bodyStr;
  }

  /**
   * Parses the HTTP response text from a request into SDC Records
   *
   * @param response HTTP response
   * @return an SDC record resulting from the response text
   */
  private boolean parseResponse(Record inRecord, InputStream response, List<Record> parsedRecords) {
    boolean close = false;
    if (conf.httpMethod == HttpMethod.HEAD) {
      // Head will have no body so can't be parsed.   Return an empty record.
      Record record = getContext().createRecord("");
      record.set(Field.create(new HashMap<>()));
      parsedRecords.add(record);
    } else if (response != null) {
      try (DataParser parser = parserFactory.getParser("", response, "0")) {
        // A response may only contain a single record, so we only parse it once.
        Record record = parser.parse();

        // LINK_FIELD pagination
        if (conf.pagination.mode == PaginationMode.LINK_FIELD) {
          // evaluate stopping condition
          RecordEL.setRecordInContext(stopVars, record);
          haveMorePages = !stopEval.eval(stopVars, conf.pagination.stopCondition, Boolean.class);
          if (haveMorePages) {
            final String nextPageURLPrefix = StringUtils.isNotBlank(conf.pagination.nextPageURLPrefix)
                                             ? conf.pagination.nextPageURLPrefix
                                             : "";
            if (!record.has(conf.pagination.nextPageFieldPath)) {
              throw new StageException(HTTP_66, getResponseStatus(), conf.pagination.nextPageFieldPath);
            }
            final String nextPageFieldValue = record.get(conf.pagination.nextPageFieldPath).getValueAsString();
            final String nextPageURL = nextPageFieldValue.startsWith(nextPageURLPrefix)
                                       ? nextPageFieldValue
                                       : nextPageURLPrefix.concat(nextPageFieldValue);
            next = Link.fromUri(nextPageURL).build();
          } else {
            next = null;
          }
        }

        if (conf.pagination.mode == PaginationMode.LINK_HEADER) {
          next = getResponse().getLink("next");
          haveMorePages = next != null;
        }

        while (record != null) {
          if (conf.dataFormat == DataFormat.TEXT) {
            // Output is placed in a field "/text" so we remove it here.
            Record rec = getContext().cloneRecord(inRecord);
            rec.set(record.get("/text"));
            parsedRecords.add(rec);
          } else if (conf.dataFormat == DataFormat.JSON || conf.dataFormat == DataFormat.XML) {
            if (record.get().getValue() instanceof List) {
              List<Field> recordList = record.get().getValueAsList();
              close |= recordList.isEmpty();
              recordList.forEach(r -> {
                Record parsedRecord = getContext().createRecord("");
                parsedRecord.set(r);
                parsedRecords.add(parsedRecord);
              });
            } else {
              //JSON Object
              if (parsePaginatedRecord(parsedRecords, record)) {
                close = true;
              }
            }
          } else {
            parsedRecords.add(record);
          }
          record = parser.parse();
        }
      } catch (IOException | DataParserException e) {
        errorRecordHandler.onError(Errors.HTTP_00, getResponseStatus(), e.toString(), e);
      }
    }

    return close;
  }

  private boolean parsePaginatedRecord(List<Record> records, Record record) {
    boolean close = false;
    if (conf.pagination.mode != PaginationMode.NONE) {
      List<Field> value = (List<Field>) record.get(conf.pagination.resultFieldPath).getValue();
      close = value.isEmpty();
    }
    if (conf.pagination.mode != PaginationMode.NONE && !conf.pagination.keepAllFields) {
      Record recordPag = getContext().createRecord("");
      recordPag.set(record.get(conf.pagination.resultFieldPath));
      records.add(recordPag);
    } else {
      records.add(record);
    }
    return close;
  }

  /**
   * Populates HTTP response headers to the configured location
   *
   * @param record current record to populate
   * @param response HTTP response
   */
  private Field createResponseHeaders(Record record, Response response) {
    Field field = null;
    if (conf.headerOutputLocation != HeaderOutputLocation.NONE) {
      Record.Header header = record.getHeader();
      header.setAttribute(REQUEST_STATUS_CONFIG_NAME, String.format("%d", response.getStatus()));
      if (conf.headerOutputLocation == HeaderOutputLocation.FIELD) {
        field = createResponseHeaderField(record, response);
      } else if (conf.headerOutputLocation == HeaderOutputLocation.HEADER) {
        createResponseHeaderToRecordHeader(response, header);
      }
    }
    return field;
  }

  /**
   * Creates the HTTP response headers to the SDC Record at the configured field path.
   *
   * @param record Record to populate with response headers.
   * @param response HTTP response
   */
  private Field createResponseHeaderField(Record record, Response response) {
    if (record.has(conf.headerOutputField)) {
      throw new StageException(Errors.HTTP_11, getResponseStatus(response), conf.headerOutputField);
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

  Response getResponse() {
    return response;
  }

  void setResponse(Response response) {
    this.response = response;
  }

  String getResponseStatus() {
    return getResponse() == null ? "NULL" : String.valueOf(getResponse().getStatus());
  }

  String getResponseStatus(Response response) {
    return response == null ? "NULL" : String.valueOf(response.getStatus());
  }
}
