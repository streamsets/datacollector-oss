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
import com.google.common.collect.ImmutableMap;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.CSVReader;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.OperationEnum;
import com.sforce.async.QueryResultList;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.bind.XmlObject;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.event.CommonEvents;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.salesforce.ForceConfigBean;
import com.streamsets.pipeline.lib.salesforce.ForceRepeatQuery;
import com.streamsets.pipeline.lib.salesforce.ForceSourceConfigBean;
import com.streamsets.pipeline.lib.salesforce.ForceUtils;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.SessionRenewer;
import com.streamsets.pipeline.lib.salesforce.Errors;
import com.streamsets.pipeline.api.base.BaseSource;
import org.cometd.bayeux.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.namespace.QName;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

public class ForceSource extends BaseSource {
  private static final long EVENT_ID_FROM_NOW = -1;
  private static final long EVENT_ID_FROM_START = -2;
  private static final String RECORD_ID_OFFSET_PREFIX = "recordId:";
  private static final String EVENT_ID_OFFSET_PREFIX = "eventId:";

  private static final Logger LOG = LoggerFactory.getLogger(ForceSource.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String HEADER_ATTRIBUTE_PREFIX = "salesforce.cdc.";
  private static final String SOBJECT_TYPE_ATTRIBUTE = "salesforce.sobjectType";
  private static final String REPLAY_ID = "replayId";
  private static final String AUTHENTICATION_INVALID = "401::Authentication invalid";
  private static final String META = "/meta";
  private static final String META_HANDSHAKE = "/meta/handshake";

  public static final String READ_EVENTS_FROM_NOW = EVENT_ID_OFFSET_PREFIX + EVENT_ID_FROM_NOW;
  public static final String READ_EVENTS_FROM_START = EVENT_ID_OFFSET_PREFIX + EVENT_ID_FROM_START;

  private static final Map<String, Integer> SFDC_TO_SDC_OPERATION = new ImmutableMap.Builder<String, Integer>()
      .put("created", OperationType.INSERT_CODE)
      .put("updated", OperationType.UPDATE_CODE)
      .put("deleted", OperationType.DELETE_CODE)
      .put("undeleted", OperationType.UNDELETE_CODE)
      .build();
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

  // SOAP API state
  private QueryResult queryResult;
  private int recordIndex;

  private long lastQueryCompletedTime = 0L;

  private BlockingQueue<Message> messageQueue;
  private ForceStreamConsumer forceConsumer;

  private Map<String, Map<String, com.sforce.soap.partner.Field>> metadataMap;
  private boolean shouldSendNoMoreDataEvent = false;
  private AtomicBoolean destroyed = new AtomicBoolean(false);

  private class FieldTree {
    String offset;
    LinkedHashMap<String, Field> map;
  }

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

    if (conf.queryExistingData) {
      final String formattedOffsetColumn = Pattern.quote(conf.offsetColumn.toUpperCase());
      Pattern offsetColumnInWhereAndOrderByClause = Pattern.compile(
          String.format("(?s).*\\bWHERE\\b.*(\\b%s\\b).*\\bORDER BY\\b.*\\b%s\\b.*",
              formattedOffsetColumn,
              formattedOffsetColumn
          )
      );

      if (!offsetColumnInWhereAndOrderByClause.matcher(conf.soqlQuery.toUpperCase()).matches()) {
        issues.add(getContext().createConfigIssue(Groups.QUERY.name(),
            ForceConfigBean.CONF_PREFIX + "soqlQuery", Errors.FORCE_07, conf.offsetColumn));
      }
    }

    if (issues.isEmpty()) {
      try {
        ConnectorConfig partnerConfig = ForceUtils.getPartnerConfig(conf, new ForceSessionRenewer());

        partnerConnection = new PartnerConnection(partnerConfig);

        bulkConnection = ForceUtils.getBulkConnection(partnerConfig, conf);

        LOG.info("Successfully authenticated as {}", conf.username);

        if (conf.queryExistingData) {
          try {
            sobjectType = ForceUtils.getSobjectTypeFromQuery(conf.soqlQuery);
            LOG.info("Found sobject type {}", sobjectType);
          } catch (StageException e) {
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
      String query = "SELECT Id, Query FROM PushTopic WHERE Name = '"+conf.pushTopic+"'";

      QueryResult qr = null;
      try {
        qr = partnerConnection.query(query);

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

      messageQueue = new ArrayBlockingQueue<>(2 * conf.basicConfig.maxBatchSize);
    }

    if (issues.isEmpty()) {
      try {
        metadataMap = ForceUtils.getMetadataMap(partnerConnection, sobjectType);
      } catch (ConnectionException e) {
        LOG.error("Exception getting metadata map: {}", e);
        issues.add(getContext().createConfigIssue(Groups.QUERY.name(),
            ForceConfigBean.CONF_PREFIX + "soqlQuery",
            Errors.FORCE_21,
            sobjectType
        ));
      }
    }

    // If issues is not empty, the UI will inform the user of each configuration issue in the list.
    return issues;
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
        e.printStackTrace();
      }
    }

    job = null;
    batch = null;

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

    try {
      metadataMap = ForceUtils.getMetadataMap(partnerConnection, sobjectType);
    } catch (ConnectionException e) {
      LOG.error("Exception getting metadata map: {}", e);
      throw new StageException(Errors.FORCE_21, sobjectType, e);
    }

    query = ForceUtils.expandWildcard(query, sobjectType, metadataMap);

    return query.replaceAll("\\$\\{offset\\}", offset);
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

  public String bulkProduce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {

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
        batch = bulkConnection.createBatchFromStream(job,
                new ByteArrayInputStream(preparedQuery.getBytes(StandardCharsets.UTF_8)));
        LOG.info("Created Bulk API batch {}", batch.getId());
      } catch (AsyncApiException e) {
        throw new StageException(Errors.FORCE_01, e);
      }
    }

    // We started the job already, see if the results are ready
    // Loop here so that we can wait for results in preview mode and not return an empty batch
    // Preview will cut us off anyway if we wait too long
    while (queryResultList == null && job != null) {
      if (destroyed.get()) {
        throw new StageException(getContext().isPreview() ? Errors.FORCE_25 : Errors.FORCE_26);
      }

      // Poll for results
      try {
        LOG.info("Waiting {} milliseconds for batch {}", conf.basicConfig.maxWaitTime, batch.getId());
        Thread.sleep(conf.basicConfig.maxWaitTime);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while sleeping");
        Thread.currentThread().interrupt();
      }
      try {
        BatchInfo info = bulkConnection.getBatchInfo(job.getId(), batch.getId());
        if (info.getState() == BatchStateEnum.Completed) {
          LOG.info("Batch {} completed", batch.getId());
          queryResultList = bulkConnection.getQueryResultList(job.getId(), batch.getId());
          LOG.info("Query results: {}", queryResultList.getResult());
          resultIndex = 0;
        } else if (info.getState() == BatchStateEnum.Failed) {
          LOG.info("Batch {} failed: {}", batch.getId(), info.getStateMessage());
          throw new StageException(Errors.FORCE_03, info.getStateMessage());
        } else if (!getContext().isPreview()) { // If we're in preview, then don't return an empty batch!
          LOG.info("Batch {} in progress", batch.getId());
          return nextSourceOffset;
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
              (resultHeader.size() > 1 || !("Records not found for this query".equals(resultHeader.get(0))))) {
        for (int i = 0; i < resultHeader.size(); i++) {
          if (resultHeader.get(i).equalsIgnoreCase(conf.offsetColumn)) {
            offsetIndex = i;
            break;
          }
        }
        if (offsetIndex == -1) {
          throw new StageException(Errors.FORCE_06, resultHeader);
        }
      }

      int numRecords = 0;
      List<String> row;
      while (numRecords < maxBatchSize) {
        try {
          if ((row = rdr.nextRecord()) == null) {
            // Exhausted this result - come back in on the next batch;
            rdr = null;
            if (resultIndex == queryResultList.getResult().length) {
              // We're out of results, too!
              try {
                bulkConnection.closeJob(job.getId());
                lastQueryCompletedTime = System.currentTimeMillis();
                LOG.info("Query completed at: {}", lastQueryCompletedTime);
              } catch (AsyncApiException e) {
                LOG.error("Error closing job: {}", e);
              }
              LOG.info("Partial batch of {} records", numRecords);
              queryResultList = null;
              batch = null;
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
            return nextSourceOffset;
          } else {
            String offset = row.get(offsetIndex);
            nextSourceOffset = RECORD_ID_OFFSET_PREFIX + offset;
            final String sourceId = conf.soqlQuery + "::" + offset;
            Record record = getContext().createRecord(sourceId);
            LinkedHashMap<String, Field> map = new LinkedHashMap<>();
            for (int i = 0; i < resultHeader.size(); i++) {
              String fieldName = resultHeader.get(i);

              // Walk the dotted list of subfields
              String[] parts = fieldName.split("\\.");

              // Process any chain of relationships
              String parent = sobjectType;
              for (int j = 0; j < parts.length - 1; j++) {
                com.sforce.soap.partner.Field sfdcField = null;
                Map<String, com.sforce.soap.partner.Field> fieldMap = metadataMap.get(parent);

                // Metadata map is indexed by field name, but it's the relationship name in the data
                for (Map.Entry<String, com.sforce.soap.partner.Field> entry : fieldMap.entrySet()) {
                  if (entry.getValue().getRelationshipName() != null) {
                    if (entry.getValue().getRelationshipName().equalsIgnoreCase(parts[j])) {
                      sfdcField = entry.getValue();
                      break;
                    }
                  }
                }

                parent = sfdcField.getReferenceTo()[0].toLowerCase();
              }

              // Now process the actual field itself
              com.sforce.soap.partner.Field sfdcField = metadataMap.get(parent).get(parts[parts.length - 1].toLowerCase());

              Field field = ForceUtils.createField(row.get(i), sfdcField);
              if (conf.createSalesforceNsHeaders) {
                ForceUtils.setHeadersOnField(field, metadataMap.get(sobjectType).get(fieldName.toLowerCase()), conf.salesforceNsHeaderPrefix);
              }
              map.put(fieldName, field);
            }
            record.set(Field.createListMap(map));
            record.getHeader().setAttribute(SOBJECT_TYPE_ATTRIBUTE, sobjectType);
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

  public String soapProduce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {

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
      final String recordContext = conf.soqlQuery + "::" + offset;

      Record rec = getContext().createRecord(recordContext);
      rec.set(Field.createListMap(ForceUtils.addFields(record, metadataMap, conf.createSalesforceNsHeaders, conf.salesforceNsHeaderPrefix)));
      rec.getHeader().setAttribute(SOBJECT_TYPE_ATTRIBUTE, sobjectType);

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

  private String processDataMessage(Message message, BatchMaker batchMaker)
      throws IOException, StageException {
    String msgJson = message.getJSON();

    // Message has the form
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
    LOG.info("Processing message: {}", msgJson);

    Object json = OBJECT_MAPPER.readValue(msgJson, Object.class);
    Map<String, Object> jsonMap = (Map<String, Object>) json;
    Map<String, Object> data = (Map<String, Object>) jsonMap.get("data");
    Map<String, Object> event = (Map<String, Object>) data.get("event");
    Map<String, Object> sobject = (Map<String, Object>) data.get("sobject");

    final String recordContext = event.get("createdDate") + "::" + sobject.get("Id");
    Record rec = getContext().createRecord(recordContext);

    // sobject data becomes fields
    LinkedHashMap<String, Field> map = new LinkedHashMap<>();

    for (String key : sobject.keySet()) {
      Object val = sobject.get(key);
      com.sforce.soap.partner.Field sfdcField = metadataMap.get(sobjectType).get(key.toLowerCase());
      Field field = ForceUtils.createField(val, sfdcField);
      if (conf.createSalesforceNsHeaders) {
        ForceUtils.setHeadersOnField(field, metadataMap.get(sobjectType).get(key.toLowerCase()), conf.salesforceNsHeaderPrefix);
      }
      map.put(key, field);
    }

    rec.set(Field.createListMap(map));

    // event data becomes header attributes
    // of the form salesforce.cdc.createdDate,
    // salesforce.cdc.type
    Record.Header recordHeader = rec.getHeader();
    for (Map.Entry<String, Object> entry : event.entrySet()) {
      recordHeader.setAttribute(HEADER_ATTRIBUTE_PREFIX + entry.getKey(), entry.getValue().toString());
      if ("type".equals(entry.getKey())) {
        int operationCode = SFDC_TO_SDC_OPERATION.get(entry.getValue().toString());
        recordHeader.setAttribute(OperationType.SDC_OPERATION_TYPE, String.valueOf(operationCode));
      }
    }
    recordHeader.setAttribute(SOBJECT_TYPE_ATTRIBUTE, sobjectType);

    batchMaker.addRecord(rec);

    return EVENT_ID_OFFSET_PREFIX + event.get(REPLAY_ID).toString();
  }

  private String streamingProduce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    String nextSourceOffset;
    if (getContext().isPreview()) {
      nextSourceOffset = READ_EVENTS_FROM_START;
    } else if (null == lastSourceOffset) {
      nextSourceOffset = READ_EVENTS_FROM_NOW;
    } else {
      nextSourceOffset = lastSourceOffset;
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

    List<Message> messages = new ArrayList<>(maxBatchSize);
    messageQueue.drainTo(messages, maxBatchSize);

    for (Message message : messages) {
      try {
        if (message.getChannel().startsWith(META)) {
          processMetaMessage(message, nextSourceOffset);
        } else {
          nextSourceOffset = processDataMessage(message, batchMaker);
        }
      } catch (IOException e) {
        throw new StageException(Errors.FORCE_02, e);
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
    job = connection.createJob(job);
    return job;
  }
}
