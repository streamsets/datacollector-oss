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
package com.streamsets.pipeline.stage.origin.salesforce;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchInfoList;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.CSVReader;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.OperationEnum;
import com.sforce.async.QueryResultList;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.SessionRenewer;
import com.sforce.ws.bind.XmlObject;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.lib.event.CommonEvents;
import com.streamsets.pipeline.lib.salesforce.BulkRecordCreator;
import com.streamsets.pipeline.lib.salesforce.Errors;
import com.streamsets.pipeline.lib.salesforce.ForceConfigBean;
import com.streamsets.pipeline.lib.salesforce.ForceRecordCreator;
import com.streamsets.pipeline.lib.salesforce.ForceRepeatQuery;
import com.streamsets.pipeline.lib.salesforce.ForceSourceConfigBean;
import com.streamsets.pipeline.lib.salesforce.ForceUtils;
import com.streamsets.pipeline.lib.salesforce.PlatformEventRecordCreator;
import com.streamsets.pipeline.lib.salesforce.PushTopicRecordCreator;
import com.streamsets.pipeline.lib.salesforce.ReplayOption;
import com.streamsets.pipeline.lib.salesforce.SoapRecordCreator;
import com.streamsets.pipeline.lib.salesforce.SobjectRecordCreator;
import com.streamsets.pipeline.lib.salesforce.SubscriptionType;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.cometd.bayeux.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import soql.SOQLParser;

import javax.xml.namespace.QName;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ForceSource extends BaseSource {
  private static final long EVENT_ID_FROM_NOW = -1;
  private static final long EVENT_ID_FROM_START = -2;
  private static final String RECORD_ID_OFFSET_PREFIX = "recordId:";
  private static final String EVENT_ID_OFFSET_PREFIX = "eventId:";

  private static final Logger LOG = LoggerFactory.getLogger(ForceSource.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String REPLAY_ID = "replayId";
  private static final String AUTHENTICATION_INVALID = "401::Authentication invalid";
  private static final String META = "/meta";
  private static final String META_HANDSHAKE = "/meta/handshake";
  private static final String READ_EVENTS_FROM_NOW = EVENT_ID_OFFSET_PREFIX + EVENT_ID_FROM_NOW;
  private static final String SFORCE_ENABLE_PKCHUNKING = "Sforce-Enable-PKChunking";
  private static final String CHUNK_SIZE = "chunkSize";
  private static final String ID = "Id";
  private static final String START_ROW = "startRow";
  private static final String RECORDS_NOT_FOUND = "Records not found for this query";

  static final String READ_EVENTS_FROM_START = EVENT_ID_OFFSET_PREFIX + EVENT_ID_FROM_START;

  private final ForceSourceConfigBean conf;

  private PartnerConnection partnerConnection;
  private BulkConnection bulkConnection;
  private String sobjectType;

  // Bulk API state
  private BatchInfo batch;
  private JobInfo job;
  private QueryResultList queryResultList;
  private int resultIndex;
  private CSVReader rdr;
  private List<String> resultHeader;
  private Set<String> processedBatches;

  // SOAP API state
  private QueryResult queryResult;
  private int recordIndex;

  private long lastQueryCompletedTime = 0L;

  private BlockingQueue<Message> messageQueue;
  private ForceStreamConsumer forceConsumer;
  private ForceRecordCreator recordCreator;

  private boolean shouldSendNoMoreDataEvent = false;
  private AtomicBoolean destroyed = new AtomicBoolean(false);

  public ForceSource(
      ForceSourceConfigBean conf
  ) {
    this.conf = conf;
  }

  // Renew the Salesforce session on timeout
  public class ForceSessionRenewer implements SessionRenewer {
    @Override
    public SessionRenewalHeader renewSession(ConnectorConfig config) throws ConnectionException {
      LOG.info("Renewing Salesforce session");

      try {
        partnerConnection = Connector.newConnection(ForceUtils.getPartnerConfig(conf, new ForceSessionRenewer()));
      } catch (StageException e) {
        throw new ConnectionException("Can't create partner config", e);
      }

      SessionRenewalHeader header = new SessionRenewalHeader();
      header.name = new QName("urn:enterprise.soap.sforce.com", "SessionHeader");
      header.headerElement = partnerConnection.getSessionHeader();
      return header;
    }
  }

  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = super.init();

    if (!conf.subscribeToStreaming && !conf.queryExistingData) {
      issues.add(
          getContext().createConfigIssue(
              Groups.FORCE.name(), ForceConfigBean.CONF_PREFIX + "queryExistingData", Errors.FORCE_00,
              "You must query existing data, subscribe for notifications, or both!"
          )
      );
    }

    if (conf.queryExistingData && conf.subscriptionType == SubscriptionType.PLATFORM_EVENT) {
      issues.add(
          getContext().createConfigIssue(
              Groups.FORCE.name(), ForceConfigBean.CONF_PREFIX + "queryExistingData", Errors.FORCE_00,
              "You cannot both query existing data and subscribe to a Platform Event!"
          )
      );
    }

    if (conf.queryExistingData) {
      SOQLParser.StatementContext statementContext = ForceUtils.getStatementContext(conf.soqlQuery);
      SOQLParser.ConditionExpressionsContext conditionExpressions = statementContext.conditionExpressions();
      SOQLParser.FieldOrderByListContext fieldOrderByList = statementContext.fieldOrderByList();

      if (conf.usePKChunking) {
        if (fieldOrderByList != null) {
          issues.add(getContext().createConfigIssue(Groups.QUERY.name(),
              ForceConfigBean.CONF_PREFIX + "soqlQuery", Errors.FORCE_31
          ));
        }

        if (conditionExpressions != null
            && checkConditionExpressions(conditionExpressions, ID)) {
          issues.add(getContext().createConfigIssue(
            Groups.QUERY.name(),
            ForceConfigBean.CONF_PREFIX + "soqlQuery", Errors.FORCE_32
          ));
        }

        if (conf.repeatQuery == ForceRepeatQuery.INCREMENTAL) {
          issues.add(getContext().createConfigIssue(Groups.QUERY.name(),
              ForceConfigBean.CONF_PREFIX + "repeatQuery", Errors.FORCE_33
          ));
        }

        conf.offsetColumn = ID;
      } else {
        if (conditionExpressions == null
            || !checkConditionExpressions(conditionExpressions, conf.offsetColumn)
            || fieldOrderByList == null
            || !checkFieldOrderByList(fieldOrderByList, conf.offsetColumn)) {
          issues.add(getContext().createConfigIssue(Groups.QUERY.name(),
              ForceConfigBean.CONF_PREFIX + "soqlQuery", Errors.FORCE_07, conf.offsetColumn
          ));
        }
      }
    }

    if (issues.isEmpty()) {
      try {
        ConnectorConfig partnerConfig = ForceUtils.getPartnerConfig(conf, new ForceSessionRenewer());

        partnerConnection = new PartnerConnection(partnerConfig);

        bulkConnection = ForceUtils.getBulkConnection(partnerConfig, conf);

        LOG.info("Successfully authenticated as {}", conf.username);

        if (conf.queryExistingData) {
          sobjectType = ForceUtils.getSobjectTypeFromQuery(conf.soqlQuery);
          LOG.info("Found sobject type {}", sobjectType);
          if (sobjectType == null) {
            issues.add(getContext().createConfigIssue(Groups.QUERY.name(),
                ForceConfigBean.CONF_PREFIX + "soqlQuery",
                Errors.FORCE_00,
                "Badly formed SOQL Query: " + conf.soqlQuery
            ));
          }
        }
      } catch (ConnectionException | AsyncApiException | StageException e) {
        LOG.error("Error connecting: {}", e);
        issues.add(getContext().createConfigIssue(Groups.FORCE.name(),
            ForceConfigBean.CONF_PREFIX + "authEndpoint",
            Errors.FORCE_00,
            ForceUtils.getExceptionCode(e) + ", " + ForceUtils.getExceptionMessage(e)
        ));
      }
    }

    if (issues.isEmpty() && conf.subscribeToStreaming) {
      if (conf.subscriptionType == SubscriptionType.PUSH_TOPIC) {
        String query = "SELECT Id, Query FROM PushTopic WHERE Name = '"+conf.pushTopic+"'";

        try {
          QueryResult qr = partnerConnection.query(query);

          if (qr.getSize() != 1) {
            issues.add(
                getContext().createConfigIssue(
                    Groups.SUBSCRIBE.name(), ForceConfigBean.CONF_PREFIX + "pushTopic", Errors.FORCE_00,
                    "Can't find Push Topic '" + conf.pushTopic +"'"
                )
            );
          } else if (null == sobjectType) {
            String soqlQuery = (String)qr.getRecords()[0].getField("Query");
            try {
              sobjectType = ForceUtils.getSobjectTypeFromQuery(soqlQuery);
              LOG.info("Found sobject type {}", sobjectType);
            } catch (StageException e) {
              issues.add(getContext().createConfigIssue(Groups.SUBSCRIBE.name(),
                  ForceConfigBean.CONF_PREFIX + "pushTopic",
                  Errors.FORCE_00,
                  "Badly formed SOQL Query: " + soqlQuery
              ));
            }
          }
        } catch (ConnectionException e) {
          issues.add(
              getContext().createConfigIssue(
                  Groups.FORCE.name(), ForceConfigBean.CONF_PREFIX + "authEndpoint", Errors.FORCE_00, e
              )
          );
        }
      }

      messageQueue = new ArrayBlockingQueue<>(2 * conf.basicConfig.maxBatchSize);
    }

    if (issues.isEmpty()) {
      recordCreator = buildRecordCreator();
    }

    // If issues is not empty, the UI will inform the user of each configuration issue in the list.
    return issues;
  }

  // Returns true if the first ORDER BY field matches fieldName
  private boolean checkFieldOrderByList(SOQLParser.FieldOrderByListContext fieldOrderByList, String fieldName) {
    return fieldOrderByList.fieldOrderByElement(0).fieldElement().getText().equalsIgnoreCase(fieldName);
  }

  // Returns true if any of the nested conditions contains fieldName
  private boolean checkConditionExpressions(
      SOQLParser.ConditionExpressionsContext conditionExpressions,
      String fieldName
  ) {
    for (SOQLParser.ConditionExpressionContext ce : conditionExpressions.conditionExpression()) {
      if ((ce.conditionExpressions() != null && checkConditionExpressions(ce.conditionExpressions(), fieldName))
          || (ce.fieldExpression() != null && ce.fieldExpression().fieldElement().getText().equalsIgnoreCase(fieldName))) {
        return true;
      }
    }

    return false;
  }

  private ForceRecordCreator buildRecordCreator() {
    if (conf.queryExistingData) {
      if (conf.useBulkAPI) {
        return new BulkRecordCreator(getContext(), conf, sobjectType);
      } else {
        return new SoapRecordCreator(getContext(), conf, sobjectType);
      }
    } else if (conf.subscribeToStreaming) {
      if (conf.subscriptionType == SubscriptionType.PUSH_TOPIC) {
        return new PushTopicRecordCreator(getContext(), conf, sobjectType);
      } else {
        return new PlatformEventRecordCreator(getContext(), conf.platformEvent);
      }
    }

    return null;
  }

  /** {@inheritDoc} */
  @Override
  public void destroy() {
    // SDC-6258 - destroy() is called from a different thread, so we
    // need to signal produce() to terminate early
    destroyed.set(true);

    if (job != null) {
      try {
        bulkConnection.abortJob(job.getId());
        job = null;
      } catch (AsyncApiException e) {
        LOG.error("Exception while aborting job", e);
      }
    }

    job = null;

    if (forceConsumer != null) {
      try {
        forceConsumer.stop();
        forceConsumer = null;
      } catch (Exception e) {
        LOG.error("Exception while stopping ForceStreamConsumer.", e);
      }

      if (!messageQueue.isEmpty()) {
        LOG.error("Queue still had {} entities at shutdown.", messageQueue.size());
      } else {
        LOG.info("Queue was empty at shutdown. No data lost.");
      }
    }

    // Clean up any open resources.
    super.destroy();
  }

  private String prepareQuery(String query, String lastSourceOffset) throws StageException {
    final String offset = (null == lastSourceOffset) ? conf.initialOffset : lastSourceOffset;
    SobjectRecordCreator sobjectRecordCreator = (SobjectRecordCreator)recordCreator;

    String expandedQuery;
    if (sobjectRecordCreator.queryHasWildcard(query)) {
      // Can't follow relationships on a wildcard query, so build the cache from the object type
      sobjectRecordCreator.buildMetadataCache(partnerConnection);
      expandedQuery = sobjectRecordCreator.expandWildcard(query);
    } else {
      // Use the query in building the cache
      expandedQuery = query;
      sobjectRecordCreator.buildMetadataCacheFromQuery(partnerConnection, query);
    }

    return expandedQuery.replaceAll("\\$\\{offset\\}", offset);
  }

  /** {@inheritDoc} */
  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    String nextSourceOffset = null;

    LOG.debug("lastSourceOffset: {}", lastSourceOffset);

    // send event only once for each time we run out of data.
    if(shouldSendNoMoreDataEvent) {
      CommonEvents.NO_MORE_DATA.create(getContext()).createAndSend();
      shouldSendNoMoreDataEvent = false;
      return lastSourceOffset;
    }

    int batchSize = Math.min(conf.basicConfig.maxBatchSize, maxBatchSize);

    if (!conf.queryExistingData ||
        (null != lastSourceOffset && lastSourceOffset.startsWith(EVENT_ID_OFFSET_PREFIX))) {
      if (conf.subscribeToStreaming) {
        nextSourceOffset = streamingProduce(lastSourceOffset, batchSize, batchMaker);
      } else {
        // We're done reading existing data, but we don't want to subscribe to Streaming API
        return null;
      }
    } else if (conf.queryExistingData) {
      if (!queryInProgress()) {
        long now = System.currentTimeMillis();
        long delay = Math.max(0, (lastQueryCompletedTime + (1000 * conf.queryInterval)) - now);

        if (delay > 0) {
          // Sleep in one second increments so we don't tie up the app.
          LOG.info("{}ms remaining until next fetch.", delay);
          ThreadUtil.sleep(Math.min(delay, 1000));

          return lastSourceOffset;
        }
      }

      if (conf.useBulkAPI) {
        nextSourceOffset = bulkProduce(lastSourceOffset, batchSize, batchMaker);
      } else {
        nextSourceOffset = soapProduce(lastSourceOffset, batchSize, batchMaker);
      }
    } else if (conf.subscribeToStreaming) {
      // No offset, and we're not querying existing data, so switch to streaming
      nextSourceOffset = READ_EVENTS_FROM_NOW;
    }

    LOG.debug("nextSourceOffset: {}", nextSourceOffset);

    return nextSourceOffset;
  }

  private boolean queryInProgress() {
    return (conf.useBulkAPI && job != null) || (!conf.useBulkAPI && queryResult != null);
  }

  private String bulkProduce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {

    String nextSourceOffset = (null == lastSourceOffset) ? RECORD_ID_OFFSET_PREFIX + conf.initialOffset : lastSourceOffset;

    if (job == null) {
      // No job in progress - start from scratch
      try {
        String id = (lastSourceOffset == null) ? null : lastSourceOffset.substring(lastSourceOffset.indexOf(':') + 1);
        final String preparedQuery = prepareQuery(conf.soqlQuery, id);
        LOG.info("SOQL Query is: {}", preparedQuery);

        if (destroyed.get()) {
          throw new StageException(getContext().isPreview() ? Errors.FORCE_25 : Errors.FORCE_26);
        }
        job = createJob(sobjectType, bulkConnection);
        LOG.info("Created Bulk API job {}", job.getId());

        if (destroyed.get()) {
          throw new StageException(getContext().isPreview() ? Errors.FORCE_25 : Errors.FORCE_26);
        }
        BatchInfo b = bulkConnection.createBatchFromStream(job,
                new ByteArrayInputStream(preparedQuery.getBytes(StandardCharsets.UTF_8)));
        LOG.info("Created Bulk API batch {}", b.getId());
        processedBatches = new HashSet<>();
      } catch (AsyncApiException e) {
        throw new StageException(Errors.FORCE_01, e);
      }
    }

    // We started the job already, see if the results are ready
    // Loop here so that we can wait for results in preview mode and not return an empty batch
    // Preview will cut us off anyway if we wait too long
    BatchInfoList batchList = null;
    while (queryResultList == null && job != null) {
      if (destroyed.get()) {
        throw new StageException(getContext().isPreview() ? Errors.FORCE_25 : Errors.FORCE_26);
      }

      try {
        // PK Chunking gives us multiple batches - process them in turn
        batchList = bulkConnection.getBatchInfoList(job.getId());
        for (BatchInfo b : batchList.getBatchInfo()) {
          if (b.getState() == BatchStateEnum.Failed) {
            LOG.error("Batch {} failed: {}", b.getId(), b.getStateMessage());
            throw new StageException(Errors.FORCE_03, b.getStateMessage());
          } else if (!processedBatches.contains(b.getId())) {
            if (b.getState() == BatchStateEnum.NotProcessed) {
              // Skip this batch - it's the 'original batch' in PK chunking
              // See https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_code_curl_walkthrough_pk_chunking.htm
              LOG.info("Batch {} not processed", b.getId());
              processedBatches.add(b.getId());
            } else if (b.getState() == BatchStateEnum.Completed) {
              LOG.info("Batch {} completed", b.getId());
              batch = b;
              queryResultList = bulkConnection.getQueryResultList(job.getId(), batch.getId());
              LOG.info("Query results: {}", queryResultList.getResult());
              resultIndex = 0;
              break;
            }
          }
        }
        if (queryResultList == null) {
          // Bulk API is asynchronous, so wait a little while...
          try {
            LOG.info("Waiting {} milliseconds for job {}", conf.basicConfig.maxWaitTime, job.getId());
            Thread.sleep(conf.basicConfig.maxWaitTime);
          } catch (InterruptedException e) {
            LOG.debug("Interrupted while sleeping");
            Thread.currentThread().interrupt();
          }
          if (!getContext().isPreview()) { // If we're in preview, then don't return an empty batch!
            LOG.info("Job {} in progress", job.getId());
            return nextSourceOffset;
          }
        }
      } catch (AsyncApiException e) {
        throw new StageException(Errors.FORCE_02, e);
      }
    }

    if (rdr == null && queryResultList != null) {
      // We have results - retrieve the next one!
      String resultId = queryResultList.getResult()[resultIndex];
      resultIndex++;

      try {
        rdr = new CSVReader(bulkConnection.getQueryResultStream(job.getId(), batch.getId(), resultId));
        rdr.setMaxRowsInFile(Integer.MAX_VALUE);
        rdr.setMaxCharsInFile(Integer.MAX_VALUE);
      } catch (AsyncApiException e) {
        throw new StageException(Errors.FORCE_05, e);
      }

      try {
        resultHeader = rdr.nextRecord();
        LOG.info("Result {} header: {}", resultId, resultHeader);
      } catch (IOException e) {
        throw new StageException(Errors.FORCE_04, e);
      }
    }

    if (rdr != null){
      int offsetIndex = -1;
      if (resultHeader != null &&
              (resultHeader.size() > 1 || !(RECORDS_NOT_FOUND.equals(resultHeader.get(0))))) {
        for (int i = 0; i < resultHeader.size(); i++) {
          if (resultHeader.get(i).equalsIgnoreCase(conf.offsetColumn)) {
            offsetIndex = i;
            break;
          }
        }
        if (offsetIndex == -1) {
          throw new StageException(Errors.FORCE_06, conf.offsetColumn, resultHeader);
        }
      }

      int numRecords = 0;
      List<String> row;
      while (numRecords < maxBatchSize) {
        try {
          if ((row = rdr.nextRecord()) == null) {
            // Exhausted this result - come back in on the next batch
            rdr = null;
            if (resultIndex == queryResultList.getResult().length) {
              // We're out of results, too!
              processedBatches.add(batch.getId());
              queryResultList = null;
              batch = null;
              if (processedBatches.size() == batchList.getBatchInfo().length) {
                // And we're done with the job
                try {
                  bulkConnection.closeJob(job.getId());
                  lastQueryCompletedTime = System.currentTimeMillis();
                  LOG.info("Query completed at: {}", lastQueryCompletedTime);
                } catch (AsyncApiException e) {
                  LOG.error("Error closing job: {}", e);
                }
                LOG.info("Partial batch of {} records", numRecords);
                job = null;
                shouldSendNoMoreDataEvent = true;
                if (conf.subscribeToStreaming) {
                  // Switch to processing events
                  nextSourceOffset = READ_EVENTS_FROM_NOW;
                } else if (conf.repeatQuery == ForceRepeatQuery.FULL) {
                  nextSourceOffset = RECORD_ID_OFFSET_PREFIX + conf.initialOffset;
                } else if (conf.repeatQuery == ForceRepeatQuery.NO_REPEAT) {
                  nextSourceOffset = null;
                }
              }
            }
            return nextSourceOffset;
          } else {
            String offset = row.get(offsetIndex);
            nextSourceOffset = RECORD_ID_OFFSET_PREFIX + offset;
            final String sourceId = conf.soqlQuery + "::" + offset;
            Record record = recordCreator.createRecord(sourceId, Pair.of(resultHeader, row));
            batchMaker.addRecord(record);
            ++numRecords;
          }
        } catch (IOException e) {
          throw new StageException(Errors.FORCE_04, e);
        }
      }
      LOG.info("Full batch of {} records", numRecords);

      return nextSourceOffset;
    }

    return nextSourceOffset;
  }

  private static XmlObject getChildIgnoreCase(SObject record, String name) {
    XmlObject item = null;
    Iterator<XmlObject> iter = record.getChildren();

    while(iter.hasNext()) {
      XmlObject child = iter.next();
      if(child.getName().getLocalPart().equalsIgnoreCase(name)) {
        item = child;
        break;
      }
    }

    return item;
  }

  private String soapProduce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {

    String nextSourceOffset = (null == lastSourceOffset) ? RECORD_ID_OFFSET_PREFIX + conf.initialOffset : lastSourceOffset;

    if (queryResult == null) {
      try {
        String id = (lastSourceOffset == null) ? null : lastSourceOffset.substring(lastSourceOffset.indexOf(':') + 1);
        final String preparedQuery = prepareQuery(conf.soqlQuery, id);
        LOG.info("preparedQuery: {}", preparedQuery);
        queryResult = conf.queryAll
            ? partnerConnection.queryAll(preparedQuery)
            : partnerConnection.query(preparedQuery);
        recordIndex = 0;
      } catch (ConnectionException e) {
        throw new StageException(Errors.FORCE_08, e);
      }
    }

    SObject[] records = queryResult.getRecords();

    LOG.info("Retrieved {} records", records.length);

    int endIndex = Math.min(recordIndex + maxBatchSize, records.length);

    for ( ;recordIndex < endIndex; recordIndex++) {
      SObject record = records[recordIndex];

      XmlObject offsetField = getChildIgnoreCase(record, conf.offsetColumn);
      if (offsetField == null || offsetField.getValue() == null) {
        throw new StageException(Errors.FORCE_22, conf.offsetColumn);
      }
      String offset = offsetField.getValue().toString();
      nextSourceOffset = RECORD_ID_OFFSET_PREFIX + offset;
      final String sourceId = conf.soqlQuery + "::" + offset;

      Record rec = recordCreator.createRecord(sourceId, record);

      batchMaker.addRecord(rec);
    }

    if (recordIndex == records.length) {
      try {
        if (queryResult.isDone()) {
          // We're out of results
          queryResult = null;
          lastQueryCompletedTime = System.currentTimeMillis();
          LOG.info("Query completed at: {}", lastQueryCompletedTime);
          shouldSendNoMoreDataEvent = true;
          if (conf.subscribeToStreaming) {
            // Switch to processing events
            nextSourceOffset = READ_EVENTS_FROM_NOW;
          } else if (conf.repeatQuery == ForceRepeatQuery.FULL) {
            nextSourceOffset = RECORD_ID_OFFSET_PREFIX + conf.initialOffset;
          } else if (conf.repeatQuery == ForceRepeatQuery.NO_REPEAT) {
            nextSourceOffset = null;
          }
        } else {
          queryResult = partnerConnection.queryMore(queryResult.getQueryLocator());
          recordIndex = 0;
        }
      } catch (ConnectionException e) {
        throw new StageException(Errors.FORCE_08, e);
      }
    }

    return nextSourceOffset;
  }

  private void processMetaMessage(Message message, String nextSourceOffset) throws StageException {
    if (message.getChannel().startsWith(META_HANDSHAKE) && message.isSuccessful()) {
      // Need to (re)subscribe after handshake - see
      // https://developer.salesforce.com/docs/atlas.en-us.api_streaming.meta/api_streaming/using_streaming_api_client_connection.htm
      try {
        forceConsumer.subscribeForNotifications(nextSourceOffset);
      } catch (InterruptedException e) {
        LOG.error(Errors.FORCE_10.getMessage(), e);
        throw new StageException(Errors.FORCE_10, e.getMessage(), e);
      }
    } else if (!message.isSuccessful()) {
      String error = (String) message.get("error");
      LOG.info("Bayeux error message: {}", error);

      if (AUTHENTICATION_INVALID.equals(error)) {
        // This happens when the session token, stored in the HttpClient as a cookie,
        // has expired, and we try to use it in a handshake.
        // Just kill the consumer and start all over again.
        LOG.info("Reconnecting after {}", AUTHENTICATION_INVALID);
        try {
          forceConsumer.stop();
        } catch (Exception ex) {
          LOG.info("Exception stopping consumer", ex);
        }
        forceConsumer = null;
      } else {
        // Some other problem...
        Exception exception = (Exception) message.get("exception");
        LOG.info("Bayeux exception:", exception);
        throw new StageException(Errors.FORCE_09, error, exception);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private String processDataMessage(Message message, BatchMaker batchMaker)
      throws IOException, StageException {
    String msgJson = message.getJSON();

    // PushTopic Message has the form
    // {
    //   "channel": "/topic/AccountUpdates",
    //   "clientId": "j17ylcz9l0t0fyp0pze7uzpqlt",
    //   "data": {
    //     "event": {
    //       "createdDate": "2016-09-15T06:01:40.000+0000",
    //       "type": "updated"
    //     },
    //     "sobject": {
    //       "AccountNumber": "3231321",
    //       "Id": "0013600000dC5xLAAS",
    //       "Name": "StreamSets",
    //       ...
    //     }
    //   }
    // }

    // Platform Event Message has the form
    //  {
    //    "data": {
    //      "schema": "dffQ2QLzDNHqwB8_sHMxdA",
    //      "payload": {
    //        "CreatedDate": "2017-04-09T18:31:40Z",
    //        "CreatedById": "005D0000001cSZs",
    //        "Printer_Model__c": "XZO-5",
    //        "Serial_Number__c": "12345",
    //        "Ink_Percentage__c": 0.2
    //      },
    //      "event": {
    //        "replayId": 2
    //      }
    //    },
    //    "channel": "/event/Low_Ink__e"
    //  }

    LOG.info("Processing message: {}", msgJson);

    Object json = OBJECT_MAPPER.readValue(msgJson, Object.class);
    Map<String, Object> jsonMap = (Map<String, Object>) json;
    Map<String, Object> data = (Map<String, Object>) jsonMap.get("data");
    Map<String, Object> event = (Map<String, Object>) data.get("event");
    Map<String, Object> sobject = (Map<String, Object>) data.get("sobject");

    final String sourceId = (conf.subscriptionType == SubscriptionType.PUSH_TOPIC)
      ? event.get("createdDate") + "::" + sobject.get("Id")
      : event.get("replayId").toString();

    Record rec = recordCreator.createRecord(sourceId, Pair.of(partnerConnection, data));

    batchMaker.addRecord(rec);

    return EVENT_ID_OFFSET_PREFIX + event.get(REPLAY_ID).toString();
  }

  private String streamingProduce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    String nextSourceOffset;
    if (getContext().isPreview()) {
      nextSourceOffset = READ_EVENTS_FROM_START;
    } else if (null == lastSourceOffset) {
      if (conf.subscriptionType == SubscriptionType.PLATFORM_EVENT &&
          conf.replayOption == ReplayOption.ALL_EVENTS) {
        nextSourceOffset = READ_EVENTS_FROM_START;
      } else {
        nextSourceOffset = READ_EVENTS_FROM_NOW;
      }
    } else {
      nextSourceOffset = lastSourceOffset;
    }

    if (recordCreator instanceof SobjectRecordCreator) {
      // Switch out recordCreator
      PushTopicRecordCreator pushTopicRecordCreator = new PushTopicRecordCreator((SobjectRecordCreator)recordCreator);
      if (!pushTopicRecordCreator.metadataCacheExists()) {
        pushTopicRecordCreator.buildMetadataCache(partnerConnection);
      }
      recordCreator = pushTopicRecordCreator;
    }

    if (forceConsumer == null) {
      // Do a request so we ensure that the connection is renewed if necessary
      try {
        partnerConnection.getUserInfo();
      } catch (ConnectionException e) {
        LOG.error("Exception getting user info", e);
        throw new StageException(Errors.FORCE_19, e.getMessage(), e);
      }

      forceConsumer = new ForceStreamConsumer(messageQueue, partnerConnection, conf);
      forceConsumer.start();
    }

    if (getContext().isPreview()) {
      // Give waiting messages a chance to arrive
      ThreadUtil.sleep(1000);
    }

    // We didn't receive any new data within the time allotted for this batch.
    if (messageQueue.isEmpty()) {
      // In preview, we already waited
      if (! getContext().isPreview()) {
        // Sleep for one second so we don't tie up the app.
        ThreadUtil.sleep(1000);
      }

      return nextSourceOffset;
    }

    // Loop if we're in preview mode - we'll get killed after the preview timeout if no data arrives
    boolean done = false;
    while (!done) {
      List<Message> messages = new ArrayList<>(maxBatchSize);
      messageQueue.drainTo(messages, maxBatchSize);

      for (Message message : messages) {
        try {
          if (message.getChannel().startsWith(META)) {
            // Handshake, subscription messages
            processMetaMessage(message, nextSourceOffset);
          } else {
            // Yay - actual data messages
            nextSourceOffset = processDataMessage(message, batchMaker);
            done = true;
          }
        } catch (IOException e) {
          throw new StageException(Errors.FORCE_02, e);
        }
      }
      if (!done && getContext().isPreview()) {
        // In preview - give data messages a chance to arrive
        if (!ThreadUtil.sleep(1000)) {
          // Interrupted!
          done = true;
        }
      } else {
        done = true;
      }
    }

    return nextSourceOffset;
  }

  private JobInfo createJob(String sobjectType, BulkConnection connection)
          throws AsyncApiException {
    JobInfo job = new JobInfo();
    job.setObject(sobjectType);
    job.setOperation(conf.queryAll ? OperationEnum.queryAll : OperationEnum.query);
    job.setContentType(ContentType.CSV);
    if (conf.usePKChunking) {
      String headerValue = CHUNK_SIZE + "=" + conf.chunkSize;
      if (!StringUtils.isEmpty(conf.startId)) {
        headerValue += "; " + START_ROW + "=" + conf.startId;
      }
      connection.addHeader(SFORCE_ENABLE_PKCHUNKING, headerValue);
    }
    job = connection.createJob(job);
    return job;
  }
}