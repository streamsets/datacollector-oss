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
package com.streamsets.pipeline.stage.origin.http;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.VaultEL;
import com.streamsets.pipeline.lib.http.Errors;
import com.streamsets.pipeline.lib.http.Groups;
import com.streamsets.pipeline.lib.http.HttpClientCommon;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.util.ExceptionUtils;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.util.http.HttpStageUtil;
import org.apache.commons.lang.StringUtils;
import org.glassfish.jersey.client.oauth1.AccessToken;
import org.glassfish.jersey.client.oauth1.OAuth1ClientSupport;
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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;
import java.nio.charset.Charset;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.streamsets.pipeline.lib.http.Errors.HTTP_21;
import static com.streamsets.pipeline.lib.http.Errors.HTTP_66;
import static com.streamsets.pipeline.lib.parser.json.Errors.JSON_PARSER_00;

/**
 * HTTP Client Origin implementation supporting streaming, polled, and paginated HTTP resources.
 */
public class HttpClientSource extends BaseSource {
  static final String START_AT = "startAt";

  private static final Logger LOG = LoggerFactory.getLogger(HttpClientSource.class);

  private static final Set<PaginationMode> LINK_PAGINATION = ImmutableSet.of(
      PaginationMode.LINK_HEADER,
      PaginationMode.LINK_FIELD
  );
  private static final int SLEEP_TIME_WAITING_FOR_BATCH_SIZE_MS = 100;
  private static final String RESOURCE_CONFIG_NAME = "resourceUrl";
  private static final String REQUEST_BODY_CONFIG_NAME = "requestBody";
  private static final String HEADER_CONFIG_NAME = "headers";
  private static final String STOP_CONFIG_NAME = "stopCondition";
  private static final String DATA_FORMAT_CONFIG_PREFIX = "conf.dataFormatConfig.";
  private static final String TLS_CONFIG_PREFIX = "conf.tlsConfig.";
  private static final String BASIC_CONFIG_PREFIX = "conf.basic.";
  private static final String VAULT_EL_PREFIX = VaultEL.PREFIX + ":";
  private static final HashFunction HF = Hashing.sha256();
  private static final String REQUEST_STATUS_CONFIG_NAME = "HTTP-Status";

  private final HttpClientConfigBean conf;
  private Hasher hasher;

  private AccessToken authToken;
  private ClientBuilder clientBuilder;
  private Client client;
  private Response response;
  private int recordCount;
  private long lastRequestCompletedTime = -1;
  private boolean lastRequestTimedOut = false;

  // Used for record id generation
  private String resolvedUrl;
  private String currentParameterHash;

  private ELVars resourceVars;
  private ELVars bodyVars;
  private ELVars headerVars;
  private ELVars stopVars;

  private ELEval resourceEval;
  private ELEval bodyEval;
  private ELEval headerEval;
  private ELEval stopEval;

  private DataParserFactory parserFactory;
  private ErrorRecordHandler errorRecordHandler;

  private Link next;
  private boolean haveMorePages;
  private DataParser parser = null;

  private long backoffIntervalLinear = 0;
  private long backoffIntervalExponential = 0;

  private int lastStatus = 0;
  private int retryCount = 0;

  private boolean checkBatchSize = true;

  private Map<Integer, HttpResponseActionConfigBean> statusToActionConfigs = new HashMap<>();
  private HttpResponseActionConfigBean timeoutActionConfig;
  private final HttpClientCommon clientCommon;
  private boolean endOfTheBatch = false;

  /**
   * @param conf Configuration object for the HTTP client
   */
  public HttpClientSource(final HttpClientConfigBean conf) {
    this.conf = conf;
    clientCommon = new HttpClientCommon(conf.client);
  }

  /** {@inheritDoc} */
  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext()); // NOSONAR

    conf.basic.init(getContext(), Groups.HTTP.name(), BASIC_CONFIG_PREFIX, issues);
    conf.dataFormatConfig.init(getContext(), conf.dataFormat, Groups.HTTP.name(), DATA_FORMAT_CONFIG_PREFIX, issues);
    conf.init(getContext(), Groups.HTTP.name(), "conf.", issues);
    if (conf.client.tlsConfig.isEnabled()) {
      conf.client.tlsConfig.init(getContext(), Groups.TLS.name(), TLS_CONFIG_PREFIX, issues);
    }

    resourceVars = getContext().createELVars();
    resourceEval = getContext().createELEval(RESOURCE_CONFIG_NAME);

    bodyVars = getContext().createELVars();
    bodyEval = getContext().createELEval(REQUEST_BODY_CONFIG_NAME);
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of(conf.timeZoneID)));
    TimeEL.setCalendarInContext(bodyVars, calendar);

    headerVars = getContext().createELVars();
    headerEval = getContext().createELEval(HEADER_CONFIG_NAME);

    stopVars = getContext().createELVars();
    stopEval = getContext().createELEval(STOP_CONFIG_NAME);

    next = null;
    haveMorePages = false;

    if (conf.responseStatusActionConfigs != null) {
      final String cfgName = "conf.responseStatusActionConfigs";

      for (HttpResponseActionConfigBean actionConfig : conf.responseStatusActionConfigs) {
        if (actionConfig.getAction() == ResponseAction.ERROR_RECORD) {
          issues.add(
              getContext().createConfigIssue(
                  Groups.HTTP.name(),
                  cfgName,
                  Errors.HTTP_37,
                  actionConfig.getAction().getLabel(),
                  actionConfig.getStatusCode()
              )
          );
        }
      }
      HttpStageUtil.validateStatusActionConfigs(
          issues,
          getContext(),
          conf.responseStatusActionConfigs,
          statusToActionConfigs,
          cfgName
      );
    }
    this.timeoutActionConfig = conf.responseTimeoutActionConfig;

    // Validation succeeded so configure the client.
    if (issues.isEmpty()) {
      try {
        configureClient(issues);
      } catch (StageException e) {
        // should not happen on initial connect
        ExceptionUtils.throwUndeclared(e);
      }
    }
    return issues;
  }

  /**
   * Helper method to apply Jersey client configuration properties.
   */
  private void configureClient(List<ConfigIssue> issues) throws StageException {

    clientCommon.init(issues, getContext());

    if (issues.isEmpty()) {
      client = clientCommon.getClient();
      parserFactory = conf.dataFormatConfig.getParserFactory();
    }

  }

  private void reconnectClient() throws StageException {
    closeHttpResources();
    client = clientCommon.buildNewClient();
  }

  /** {@inheritDoc} */
  @Override
  public void destroy() {
    closeHttpResources();
    clientBuilder = null;
    super.destroy();
  }

  private void closeHttpResources() {
    if (response != null) {
      response.close();
      response = null;
    }
    if (client != null) {
      client.close();
      client = null;
    }
  }

  /** {@inheritDoc} */
  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    long start = System.currentTimeMillis();
    int chunksToFetch = Math.min(conf.basic.maxBatchSize, maxBatchSize);
    if (!getContext().isPreview() && checkBatchSize && conf.basic.maxBatchSize > maxBatchSize) {
      getContext().reportError(Errors.HTTP_35, maxBatchSize);
      checkBatchSize = false;
    }

    Optional<String> newSourceOffset = Optional.empty();
    recordCount = 0;

    setPageOffset(lastSourceOffset);

    setResolvedUrl(resolveInitialUrl(lastSourceOffset));
    WebTarget target = client.target(getResolvedUrl());

    // If the request (headers or body) contain a known sensitive EL and we're not using https then fail the request.
    if (requestContainsSensitiveInfo() && !target.getUri().getScheme().toLowerCase().startsWith("https")) {
      LOG.error(Errors.HTTP_07.getMessage());
      throw new StageException(Errors.HTTP_07);
    }

    boolean uninterrupted = true;

    while (!waitTimeExpired(start) && uninterrupted && (recordCount < chunksToFetch)) {
      if (parser != null) {
        // We already have an response that we haven't finished reading.
        newSourceOffset = Optional.of(parseResponse(start, chunksToFetch, batchMaker));
      } else if (shouldMakeRequest()) {

        if (conf.pagination.mode != PaginationMode.NONE) {
          target = client.target(resolveNextPageUrl(newSourceOffset.orElse(null)));
          // Pause between paging requests so we don't get rate limited.
          uninterrupted = ThreadUtil.sleep(conf.pagination.rateLimit);
        }

        makeRequest(target);
        if (lastRequestTimedOut) {
          String actionName = conf.responseTimeoutActionConfig.getAction().name();
          LOG.warn(
              "HTTPClient timed out after waiting {} ms for response from server;" +
              " reconnecting client and proceeding as per configured {} action",
              conf.client.readTimeoutMillis,
              actionName
          );
          reconnectClient();
          return nonTerminating(lastSourceOffset);
        } else {
          newSourceOffset = processResponse(start, chunksToFetch, batchMaker);
        }
      } else if (conf.httpMode == HttpClientMode.BATCH) {
        // We are done.
        return null;
      } else {
        // In polling mode, waiting for the next polling interval.
        uninterrupted = ThreadUtil.sleep(SLEEP_TIME_WAITING_FOR_BATCH_SIZE_MS);
      }
    }

    return newSourceOffset.orElse(lastSourceOffset);
  }

  @VisibleForTesting
  String resolveInitialUrl(String storedOffset) throws ELEvalException {
    if (LINK_PAGINATION.contains(conf.pagination.mode) && StringUtils.isNotBlank(storedOffset)) {
      final HttpSourceOffset offset = HttpSourceOffset.fromString(storedOffset);
      return offset.getUrl();
    } else {
      return resourceEval.eval(resourceVars, conf.resourceUrl, String.class);
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
  @VisibleForTesting
  String resolveNextPageUrl(String sourceOffset) throws ELEvalException {
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
    if (StringUtils.isNotEmpty(sourceOffset)) {
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

    MultivaluedMap<String, Object> resolvedHeaders = resolveHeaders();
    final Invocation.Builder invocationBuilder = target
        .request()
        .property(OAuth1ClientSupport.OAUTH_PROPERTY_ACCESS_TOKEN, authToken)
        .headers(resolvedHeaders);

    boolean keepRequesting = !getContext().isStopped();
    boolean gotNewToken = false;
    while (keepRequesting) {
      long startTime = System.currentTimeMillis();
      try {
        if (conf.requestBody != null && !conf.requestBody.isEmpty() && conf.httpMethod != HttpMethod.GET) {
          final String requestBody = bodyEval.eval(bodyVars, conf.requestBody, String.class);
          final String contentType = HttpStageUtil.getContentTypeWithDefault(
              resolvedHeaders, conf.defaultRequestContentType);
          hasher.putString(requestBody, Charset.forName(conf.dataFormatConfig.charset));
          setResponse(invocationBuilder.method(conf.httpMethod.getLabel(), Entity.entity(requestBody, contentType)));
        } else {
          setResponse(invocationBuilder.method(conf.httpMethod.getLabel()));
        }
        LOG.debug("Retrieved response in {} ms", System.currentTimeMillis() - startTime);

        lastRequestTimedOut = false;
        final int status = response.getStatus();
        final boolean statusOk = status >= 200 && status < 300;
        if (conf.client.useOAuth2 && (status == 403 || status == 401)) { // Token may have expired
          if (gotNewToken) {
            LOG.error(HTTP_21.getMessage());
            throw new StageException(HTTP_21);
          }
          gotNewToken = HttpStageUtil.getNewOAuth2Token(conf.client.oauth2, client);
        } else if (!statusOk && this.statusToActionConfigs.containsKey(status)) {
          final HttpResponseActionConfigBean actionConf = this.statusToActionConfigs.get(status);
          final boolean statusChanged = lastStatus != status || lastRequestTimedOut;

          final AtomicInteger retryCountObj = new AtomicInteger(retryCount);
          final AtomicLong backoffExp = new AtomicLong(backoffIntervalExponential);
          final AtomicLong backoffLin = new AtomicLong(backoffIntervalLinear);
          HttpStageUtil.applyResponseAction(
              actionConf,
              statusChanged,
              input -> new StageException(Errors.HTTP_14, status, response.readEntity(String.class)),
              retryCountObj,
              backoffLin,
              backoffExp
          );
          this.retryCount = retryCountObj.get();
          this.backoffIntervalExponential = backoffExp.get();
          this.backoffIntervalLinear = backoffLin.get();

        } else {
          keepRequesting = false;
          retryCount = 0;
        }
        lastStatus = status;
      } catch (Exception e) {
        LOG.debug("Request failed after {} ms", System.currentTimeMillis() - startTime);
        final Throwable cause = e.getCause();
        if (cause != null && (cause instanceof TimeoutException || cause instanceof SocketTimeoutException)) {
          LOG.warn(
              "{} attempting to read response in HttpClientSource: {}",
              cause.getClass().getSimpleName(),
              e.getMessage(),
              e
          );
          // read timeout; consult configured action to decide on backoff and retry strategy
          if (this.timeoutActionConfig != null) {
            final HttpResponseActionConfigBean actionConf = this.timeoutActionConfig;

            final boolean firstTimeout = !lastRequestTimedOut;

            final AtomicInteger retryCountObj = new AtomicInteger(retryCount);
            final AtomicLong backoffExp = new AtomicLong(backoffIntervalExponential);
            final AtomicLong backoffLin = new AtomicLong(backoffIntervalLinear);
            HttpStageUtil.applyResponseAction(
                actionConf,
                firstTimeout,
                input -> new StageException(Errors.HTTP_18),
                retryCountObj,
                backoffLin,
                backoffExp
            );
            this.retryCount = retryCountObj.get();
            this.backoffIntervalExponential = backoffExp.get();
            this.backoffIntervalLinear = backoffLin.get();

          }

          lastRequestTimedOut = true;
          keepRequesting = false;
        } else if (cause != null && cause instanceof InterruptedException) {
          LOG.error(
            String.format(
                "InterruptedException attempting to make request in HttpClientSource; stopping: %s",
                e.getMessage()
            ),
          e);
          keepRequesting = false;
        } else {
          LOG.error(
            String.format(
                "ProcessingException attempting to make request in HttpClientSource: %s",
                e.getMessage()
            ),
          e);
          Throwable reportEx = cause != null ? cause : e;
          final StageException stageException = new StageException(Errors.HTTP_32, getResponseStatus(), reportEx.toString(), reportEx);
          LOG.error(stageException.getMessage());
          throw stageException;
        }
      }
      keepRequesting &= !getContext().isStopped();
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
    final long now = System.currentTimeMillis();

    boolean shouldMakeRequest = lastRequestCompletedTime == -1;
    shouldMakeRequest |= lastRequestTimedOut;
    shouldMakeRequest |= next != null;
    shouldMakeRequest |= (haveMorePages && conf.pagination.mode != PaginationMode.LINK_HEADER);
    shouldMakeRequest |= now > lastRequestCompletedTime + conf.pollingInterval &&
        conf.httpMode == HttpClientMode.POLLING;
    shouldMakeRequest |= now > lastRequestCompletedTime && conf.httpMode == HttpClientMode.STREAMING && conf.httpMethod != HttpMethod.HEAD;
    shouldMakeRequest &= !endOfTheBatch;

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
  @VisibleForTesting
  String parseResponse(long start, int maxRecords, BatchMaker batchMaker) throws StageException {
    HttpSourceOffset sourceOffset = new HttpSourceOffset(
        getResolvedUrl(),
        currentParameterHash,
        System.currentTimeMillis(),
        getCurrentPage()
    );
    InputStream in = null;
    if (parser == null) {
      // Only get a new parser if we are done with the old one.
      in = getResponse().readEntity(InputStream.class);
      try {
        parser = parserFactory.getParser(sourceOffset.toString(), in, "0");
      } catch (DataParserException e) {
        if (e.getErrorCode() == JSON_PARSER_00) {
          LOG.warn("No data returned in HTTP response body.", e);
          return sourceOffset.toString();
        }
        LOG.warn("Error parsing response", e);
        throw e;
      }
    }

    Record record = null;
    int subRecordCount = 0;
    try {

      do {
        record = parser.parse();

        if (record == null) {
          break;
        }
        record.getHeader().setAttribute(REQUEST_STATUS_CONFIG_NAME, String.format("%d",getResponse().getStatus()));

        // LINK_FIELD pagination
        if (conf.pagination.mode == PaginationMode.LINK_FIELD) {
          // evaluate stopping condition
          RecordEL.setRecordInContext(stopVars, record);
          haveMorePages = !stopEval.eval(stopVars, conf.pagination.stopCondition, Boolean.class);
          if (haveMorePages) {
            final String nextPageURLPrefix = StringUtils.isNotBlank(conf.pagination.nextPageURLPrefix) ? conf.pagination.nextPageURLPrefix : "";
            if(!record.has(conf.pagination.nextPageFieldPath)){
              throw new StageException(HTTP_66, getResponseStatus(), conf.pagination.nextPageFieldPath);
            }
            final String nextPageFieldValue = record.get(conf.pagination.nextPageFieldPath).getValueAsString();
            final String nextPageURL = nextPageFieldValue.startsWith(nextPageURLPrefix) ? nextPageFieldValue : nextPageURLPrefix.concat(nextPageFieldValue);
            next = Link.fromUri(nextPageURL).build();
          } else {
            next = null;
          }
        }

        if (conf.pagination.mode != PaginationMode.NONE && record.has(conf.pagination.resultFieldPath)) {
          subRecordCount = parsePaginatedResult(batchMaker, sourceOffset.toString(), record);
          recordCount += subRecordCount;
        } else {
          addResponseHeaders(record.getHeader());
          batchMaker.addRecord(record);
          ++recordCount;
        }

      } while (recordCount < maxRecords && !waitTimeExpired(start));

    } catch (IOException e) {
      LOG.error(Errors.HTTP_00.getMessage(), getResponseStatus(), e.toString(), e);
      errorRecordHandler.onError(Errors.HTTP_00, getResponseStatus(), e.toString(), e);

    } finally {
      try {
        if (record == null) {
          cleanupResponse(in);
        }
        if (subRecordCount != 0) {
          incrementSourceOffset(sourceOffset, subRecordCount);
        }
      } catch(IOException e) {
        LOG.warn(Errors.HTTP_28.getMessage(), getResponseStatus(), e.toString(), e);
        errorRecordHandler.onError(Errors.HTTP_28, getResponseStatus(), e.toString(), e);
      }
    }

    return sourceOffset.toString();
  }

  @VisibleForTesting
  Response getResponse() {
    return response;
  }

  String getResponseStatus(){
    if(getResponse() == null){
      return "NULL";
    }
    return String.format("%d",getResponse().getStatus());
  }

  @VisibleForTesting
  void setResponse(Response response) {
    this.response = response;
  }

  /**
   * Used only for HEAD requests.  Sets up a record for output based on headers only
   * with an empty body.
   *
   * @param batchMaker batch to add records to.
   * @return the next source offset to commit
   * @throws StageException if an unhandled error is encountered
   */

  String parseHeadersOnly(BatchMaker batchMaker) throws StageException {
    HttpSourceOffset sourceOffset = new HttpSourceOffset(
            getResolvedUrl(),
            currentParameterHash,
            System.currentTimeMillis(),
            getCurrentPage()
    );

    Record record = getContext().createRecord(sourceOffset + "::0");
    addResponseHeaders(record.getHeader());
    record.set(Field.create(new HashMap()));

    batchMaker.addRecord(record);
    recordCount++;
    incrementSourceOffset(sourceOffset, 1);
    lastRequestCompletedTime = System.currentTimeMillis();
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
    IOException ex = null;

    LOG.debug("Cleanup after request processing complete.");
    lastRequestCompletedTime = System.currentTimeMillis();

    if (in != null) {
      try {
        in.close();
      } catch(IOException e) {
        LOG.warn("Error closing input stream", ex);
        ex = e;
      }
    }

    getResponse().close();
    setResponse(null);

    try {
      parser.close();
    } catch(IOException e) {
      LOG.warn("Error closing parser", ex);
      ex = e;
    }
    parser = null;

    if(ex != null) {
      throw ex;
    }
  }

  /**
   * Returns the most recently requested page number or page offset requested.
   *
   * @return page number or offset
   */
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
      final StageException stageException = new StageException(Errors.HTTP_12, getResponseStatus(), conf.pagination.resultFieldPath);
      LOG.error(stageException.getMessage());
      throw stageException;
    }
    Field resultField = record.get(conf.pagination.resultFieldPath);

    if (resultField.getType() != Field.Type.LIST) {
      final StageException stageException = new StageException(Errors.HTTP_08, getResponseStatus(), resultField.getType());
      LOG.error(stageException.getMessage());
      throw stageException;
    }

    List<Field> results = resultField.getValueAsList();
    int subRecordIdx = 0;
    for (Field result : results) {
      Record r = getContext().createRecord(sourceOffset + "::" + subRecordIdx++);
      if (conf.pagination.keepAllFields) {
        r.set(record.get().clone());
        r.set(conf.pagination.resultFieldPath, result);
      } else {
        r.set(result);
      }
      addResponseHeaders(r.getHeader());
      batchMaker.addRecord(r);
      ++numSubRecords;
    }
    if (conf.pagination.mode != PaginationMode.LINK_FIELD) {
      haveMorePages = numSubRecords > 0;
    }
    return numSubRecords;
  }

  /**
   * Adds the HTTP response headers to the record header.
   * @param header an SDC record header to populate
   */
  private void addResponseHeaders(Record.Header header) {
    final MultivaluedMap<String, String> headers = getResponse().getStringHeaders();
    if (headers == null) {
      return;
    }

    for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
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
    Optional<String> newSourceOffset = Optional.empty();

    if (getResponse() == null) {
      return newSourceOffset;
    }

    // Response was not in the OK range, so treat as an error
    int status = getResponse().getStatus();
    if (status < 200 || status >= 300) {
      lastRequestCompletedTime = System.currentTimeMillis();
      String reason = getResponse().getStatusInfo().getReasonPhrase();
      String respString = getResponse().readEntity(String.class);

      final String errorMsg = reason + " : " + respString;
      LOG.warn(Errors.HTTP_01.getMessage(), status, errorMsg);

      if(conf.propagateAllHttpResponses){
        Map<String,Field> mapFields = new HashMap<>();
        mapFields.put(conf.errorResponseField,Field.create(respString));
        Record r = getContext().createRecord("");
        r.set(Field.create(mapFields));
        addResponseHeaders(r.getHeader());
        r.getHeader().setAttribute(REQUEST_STATUS_CONFIG_NAME, String.format("%d",getResponse().getStatus()));
        batchMaker.addRecord(r);
      }else{
        errorRecordHandler.onError(Errors.HTTP_01, status, errorMsg);
      }

      getResponse().close();
      setResponse(null);


      return newSourceOffset;
    }

    if (conf.pagination.mode == PaginationMode.LINK_HEADER) {
      next = getResponse().getLink("next");
      if (next == null) {
        haveMorePages = false;
      }
    }


    if (getResponse().hasEntity()) {
      newSourceOffset = Optional.of(parseResponse(start, maxRecords, batchMaker));
    } else if (conf.httpMethod.getLabel() == "HEAD") {
      // Handle HEAD only requests, which have no body, by creating a blank record for output with headers.
      newSourceOffset = Optional.of(parseHeadersOnly(batchMaker));

    }else if(conf.httpMode == HttpClientMode.BATCH){
      endOfTheBatch = true;
    }
    return newSourceOffset;
  }

  protected String nonTerminating(String sourceOffset) {
    return sourceOffset == null ? "" : sourceOffset;
  }

  @VisibleForTesting
  String getResolvedUrl() {
    return resolvedUrl;
  }

  @VisibleForTesting
  void setResolvedUrl(String resolvedUrl) {
    this.resolvedUrl = resolvedUrl;
  }
}
