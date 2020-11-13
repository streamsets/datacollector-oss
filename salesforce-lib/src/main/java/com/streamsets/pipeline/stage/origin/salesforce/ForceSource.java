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
import com.sforce.async.BulkConnection;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.SessionRenewer;
import com.sforce.ws.bind.XmlObject;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.lib.event.NoMoreDataEvent;
import com.streamsets.pipeline.lib.salesforce.BulkRecordCreator;
import com.streamsets.pipeline.lib.salesforce.ChangeDataCaptureRecordCreator;
import com.streamsets.pipeline.lib.salesforce.Errors;
import com.streamsets.pipeline.lib.salesforce.ForceBulkReader;
import com.streamsets.pipeline.lib.salesforce.ForceCollector;
import com.streamsets.pipeline.lib.salesforce.ForceConfigBean;
import com.streamsets.pipeline.lib.salesforce.ForceInputConfigBean;
import com.streamsets.pipeline.lib.salesforce.ForceRecordCreator;
import com.streamsets.pipeline.lib.salesforce.ForceRepeatQuery;
import com.streamsets.pipeline.lib.salesforce.ForceSourceConfigBean;
import com.streamsets.pipeline.lib.salesforce.ForceStage;
import com.streamsets.pipeline.lib.salesforce.ForceUtils;
import com.streamsets.pipeline.lib.salesforce.PlatformEventRecordCreator;
import com.streamsets.pipeline.lib.salesforce.PushTopicRecordCreator;
import com.streamsets.pipeline.lib.salesforce.ReplayOption;
import com.streamsets.pipeline.lib.salesforce.SoapRecordCreator;
import com.streamsets.pipeline.lib.salesforce.SobjectRecordCreator;
import com.streamsets.pipeline.lib.salesforce.SubscriptionType;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.cometd.bayeux.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import soql.SOQLParser;

import javax.xml.namespace.QName;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ForceSource extends BaseSource implements ForceStage {

  private static final Logger LOG = LoggerFactory.getLogger(ForceSource.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String REPLAY_ID = "replayId";
  private static final String AUTHENTICATION_INVALID = "401::Authentication invalid";
  private static final String META = "/meta";
  private static final String META_HANDSHAKE = "/meta/handshake";
  private static final String META_CONNECT = "/meta/connect";
  private static final String ID = "Id";
  private static final String CONF_PREFIX = "conf";
  private static final String COUNT = "count";

  private static final BigDecimal MAX_OFFSET_INT = new BigDecimal(Integer.MAX_VALUE);
  private static final String NONE = "none";
  private static final String RECONNECT = "reconnect";

  private final ForceSourceConfigBean conf;

  private PartnerConnection partnerConnection;
  private BulkConnection bulkConnection;
  private String sobjectType;

  // SOAP API state
  private QueryResult queryResult;
  private int recordIndex;

  private long lastQueryCompletedTime = 0L;

  private BlockingQueue<Message> messageQueue;
  private ForceStreamConsumer forceConsumer;
  private ForceRecordCreator recordCreator;

  private boolean queryComplete = false;   // true if there are no more records to read from the current query
  private boolean noRecordsCreated = true; // true if we haven't created any records from the current query
  private boolean eventFired = false;      // true if we have fired an event since the last query
  private AtomicBoolean destroyed = new AtomicBoolean(false);
  private ForceBulkReader bulkReader;
  private static final String datePattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
  private SimpleDateFormat dateFormat = new SimpleDateFormat(datePattern);

  private long noMoreDataRecordCount = 0;

  public ForceSource(
      ForceSourceConfigBean conf
  ) {
    this.conf = conf;
  }

  @Override
  public BulkConnection getBulkConnection() {
    return bulkConnection;
  }

  @Override
  public ForceRecordCreator getRecordCreator() {
    return recordCreator;
  }

  @Override
  public ForceInputConfigBean getConfig() {
    return conf;
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
    Optional
        .ofNullable(conf.init(getContext(), CONF_PREFIX))
        .ifPresent(issues::addAll);

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
      if (conf.bulkConfig.usePKChunking) {
        conf.offsetColumn = ID;
      }

      if (!conf.disableValidation) {
        try {
          SOQLParser.StatementContext statementContext = ForceUtils.getStatementContext(conf.soqlQuery);
          SOQLParser.ConditionExpressionsContext conditionExpressions = statementContext.conditionExpressions();
          SOQLParser.FieldOrderByListContext fieldOrderByList = statementContext.fieldOrderByList();

          String sobject = statementContext.objectList().getText().toLowerCase();

          if (conf.bulkConfig.usePKChunking) {
            if (fieldOrderByList != null) {
              issues.add(getContext().createConfigIssue(Groups.QUERY.name(),
                  ForceConfigBean.CONF_PREFIX + "soqlQuery", Errors.FORCE_31
              ));
            }

            if (conditionExpressions != null && checkConditionExpressions(conditionExpressions, sobject, ID)) {
              issues.add(getContext().createConfigIssue(Groups.QUERY.name(),
                  ForceConfigBean.CONF_PREFIX + "soqlQuery",
                  Errors.FORCE_32
              ));
            }

            if (conf.repeatQuery == ForceRepeatQuery.INCREMENTAL) {
              issues.add(getContext().createConfigIssue(Groups.QUERY.name(),
                  ForceConfigBean.CONF_PREFIX + "repeatQuery", Errors.FORCE_33
              ));
            }
          } else {
            if (conditionExpressions == null || !checkConditionExpressions(conditionExpressions, sobject,
                conf.offsetColumn
            ) || fieldOrderByList == null || !checkFieldOrderByList(fieldOrderByList, sobject, conf.offsetColumn)) {
              issues.add(getContext().createConfigIssue(Groups.QUERY.name(),
                  ForceConfigBean.CONF_PREFIX + "soqlQuery", Errors.FORCE_07, conf.offsetColumn
              ));
            }
          }
        } catch (ParseCancellationException e) {
          LOG.error(Errors.FORCE_27.getMessage(), conf.soqlQuery, e);
          issues.add(getContext().createConfigIssue(Groups.QUERY.name(),
              ForceConfigBean.CONF_PREFIX + "soqlQuery", Errors.FORCE_27, conf.soqlQuery, e
          ));
        }
      }
    }

    if (!conf.disableValidation && StringUtils.isEmpty(conf.initialOffset)) {
      issues.add(
          getContext().createConfigIssue(
              Groups.FORCE.name(), ForceConfigBean.CONF_PREFIX + "initialOffset", Errors.FORCE_00,
              "You must configure an Initial Offset"
          )
      );
    }

    if (!conf.disableValidation && StringUtils.isEmpty(conf.offsetColumn)) {
      issues.add(
          getContext().createConfigIssue(
              Groups.FORCE.name(), ForceConfigBean.CONF_PREFIX + "queryExistingData", Errors.FORCE_00,
              "You must configure an Offset Column"
          )
      );
    }

    if (issues.isEmpty()) {
      try {
        ConnectorConfig partnerConfig = ForceUtils.getPartnerConfig(conf, new ForceSessionRenewer());

        partnerConnection = new PartnerConnection(partnerConfig);
        if (conf.connection.mutualAuth.useMutualAuth) {
          ForceUtils.setupMutualAuth(partnerConfig, conf.connection.mutualAuth);
        }

        bulkConnection = ForceUtils.getBulkConnection(partnerConfig, conf);

        LOG.info("Successfully authenticated as {}", conf.connection.username);

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
      } catch (ConnectionException | AsyncApiException | StageException | URISyntaxException e) {
        LOG.error("Error connecting", e);
        issues.add(getContext().createConfigIssue(Groups.FORCE.name(),
            ForceConfigBean.CONF_PREFIX + "authEndpoint",
            Errors.FORCE_00,
            ForceUtils.getExceptionCode(e) + ", " + ForceUtils.getExceptionMessage(e)
        ));
      }
    }

    if (issues.isEmpty() && conf.subscribeToStreaming) {
      // Push topic sets the sobject type here so that streamingProduce can populate the metadata cache - PushTopic
      // notifications do not include the sobject type
      // Platform events doesn't use sobjects
      // CDC gets object type with each update
      if (conf.subscriptionType == SubscriptionType.PUSH_TOPIC) {
        setObjectTypeFromQuery(issues);
      }

      messageQueue = new ArrayBlockingQueue<>(2 * conf.basicConfig.maxBatchSize);
    }

    if (issues.isEmpty()) {
      recordCreator = buildRecordCreator();
      try {
        recordCreator.init();
      } catch (StageException e) {
        issues.add(getContext().createConfigIssue(null, null, Errors.FORCE_34, e));
      }

      if (conf.queryExistingData && conf.useBulkAPI) {
        bulkReader = new ForceBulkReader(this);
      }
    }

    queryComplete = false;
    noRecordsCreated = true;
    eventFired = false;

    // If issues is not empty, the UI will inform the user of each configuration issue in the list.
    return issues;
  }

  private void setObjectTypeFromQuery(List<ConfigIssue> issues) {
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

  // Returns true if the first ORDER BY field matches fieldName
  private boolean checkFieldOrderByList(SOQLParser.FieldOrderByListContext fieldOrderByList, String objectName, String fieldName) {
    String objectFieldName = objectName + "." + fieldName;
    String orderByField = fieldOrderByList.fieldOrderByElement(0).fieldElement().getText();

    return orderByField.equalsIgnoreCase(fieldName) || orderByField.equalsIgnoreCase(objectFieldName);
  }

  // Returns true if any of the nested conditions contains fieldName, optionally preceded by objectName
  private boolean checkConditionExpressions(
      SOQLParser.ConditionExpressionsContext conditionExpressions, String objectName, String fieldName
  ) {
    String objectFieldName = objectName + "." + fieldName;

    for (SOQLParser.ConditionExpressionContext ce : conditionExpressions.conditionExpression()) {
      if (ce.conditionExpressions() != null && checkConditionExpressions(ce.conditionExpressions(), objectName, fieldName)) {
        return true;
      } else if (ce.fieldExpression() != null){
        String conditionField = ce.fieldExpression().fieldElement().getText();

        return conditionField.equalsIgnoreCase(fieldName) || conditionField.equalsIgnoreCase(objectFieldName);
      }
    }

    return false;
  }

  private ForceRecordCreator buildRecordCreator() {
    if (conf.queryExistingData) {
      if (conf.useBulkAPI) {
        //IMPORTANT: BulkRecordCreator is not thread safe since it is using SimpleDateFormat
        return new BulkRecordCreator(getContext(), conf, sobjectType);
      } else {
        return new SoapRecordCreator(getContext(), conf, sobjectType);
      }
    } else if (conf.subscribeToStreaming) {
      switch (conf.subscriptionType) {
        case PUSH_TOPIC:
          return new PushTopicRecordCreator(getContext(), conf, sobjectType);
        case PLATFORM_EVENT:
          return new PlatformEventRecordCreator(getContext(), conf.platformEvent, conf);
        case CDC:
          return new ChangeDataCaptureRecordCreator(getContext(), conf);
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

    if (bulkReader != null) {
      bulkReader.destroy();
    }

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

    if (recordCreator != null) {
      recordCreator.destroy();
    }

    // Clean up any open resources.
    super.destroy();
  }

  public String prepareQuery(String query, String offset) throws StageException {
    String id = offset.substring(offset.indexOf(':') + 1);
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

    return (offset == null) ? expandedQuery : expandedQuery.replaceAll("\\$\\{offset\\}", id);
  }

  /** {@inheritDoc} */
  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    String nextSourceOffset = null;

    LOG.debug("lastSourceOffset: {}", lastSourceOffset);

    int batchSize = Math.min(conf.basicConfig.maxBatchSize, maxBatchSize);

    if (!conf.queryExistingData ||
        (null != lastSourceOffset && lastSourceOffset.startsWith(ForceUtils.EVENT_ID_OFFSET_PREFIX))) {
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
          LOG.debug("{}ms remaining until next fetch.", delay);
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
      nextSourceOffset = ForceUtils.READ_EVENTS_FROM_NOW;
    }

    // If we're not repeating the query, then send the event
    // If we ARE repeating, wait for a query with no records (SDC-12418)
    if (queryComplete && !eventFired && (conf.repeatQuery == ForceRepeatQuery.NO_REPEAT || noRecordsCreated)) {
      NoMoreDataEvent.EVENT_CREATOR.create(getContext())
          .with(NoMoreDataEvent.RECORD_COUNT, noMoreDataRecordCount)
          .createAndSend();
      noMoreDataRecordCount = 0;
      eventFired = true;
    }

    LOG.debug("nextSourceOffset: {}", nextSourceOffset);

    return nextSourceOffset;
  }

  private boolean queryInProgress() {
    return (conf.useBulkAPI && bulkReader.queryInProgress()) || (!conf.useBulkAPI && queryResult != null);
  }

  private static Object getIgnoreCase(Map<String, Field> map, String offsetColumn) {
    for (Map.Entry<String, Field> entry : map.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(offsetColumn)) {
        return entry.getValue().getValue();
      }
    }

    return null;
  }

  private String bulkProduce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    String nextSourceOffset = (null == lastSourceOffset) ? ForceUtils.RECORD_ID_OFFSET_PREFIX + conf.initialOffset : lastSourceOffset;

    return bulkReader.produce(nextSourceOffset, maxBatchSize, new ForceCollector() {
      @Override
      public void init() {
        noRecordsCreated = true;
        queryComplete = false;
      }

      @Override
      public String addRecord(LinkedHashMap<String, Field> fields, int numRecords) {
        noRecordsCreated = false;
        eventFired = false;

        // Get the offset from the record
        Object newRawOffset = getIgnoreCase(fields, conf.offsetColumn);
        if (newRawOffset == null) {
          throw new StageException(Errors.FORCE_22, conf.offsetColumn);
        }
        String newOffset;
        if (newRawOffset instanceof Date){
          newOffset = dateFormat.format((Date) newRawOffset);
        } else {
          newOffset = newRawOffset.toString();
        }

        final String sourceId = StringUtils.substring(conf.soqlQuery.replaceAll("[\n\r]", " "), 0, 100) + "::rowCount:" + recordIndex + (StringUtils.isEmpty(conf.offsetColumn) ? "" : ":" + newOffset);
        batchMaker.addRecord(recordCreator.createRecord(sourceId, fields));
        noMoreDataRecordCount++;
        recordIndex++;

        return fixOffset(conf.offsetColumn, newOffset);
      }

      @Override
      public String prepareQuery(String offset) {
        return ForceSource.this.prepareQuery(conf.soqlQuery, offset);
      }

      @Override
      public boolean isDestroyed() {
        return destroyed.get();
      }

      @Override
      public boolean isPreview() {
        return getContext().isPreview();
      }

      @Override
      public void queryCompleted() {
        lastQueryCompletedTime = System.currentTimeMillis();
        LOG.info("Query completed at: {}", lastQueryCompletedTime);
        queryComplete = true;
        recordIndex = 0;
      }

      @Override
      public boolean subscribeToStreaming() {
        return conf.subscribeToStreaming;
      }

      @Override
      public ForceRepeatQuery repeatQuery() {
        return conf.repeatQuery;
      }

      @Override
      public String initialOffset() {
        return conf.initialOffset;
      }
    });
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

    String nextSourceOffset = (null == lastSourceOffset) ? ForceUtils.RECORD_ID_OFFSET_PREFIX + conf.initialOffset : lastSourceOffset;

    if (queryResult == null) {
      try {
        final String preparedQuery = prepareQuery(conf.soqlQuery, nextSourceOffset);
        LOG.info("preparedQuery: {}", preparedQuery);
        queryResult = conf.queryAll
            ? partnerConnection.queryAll(preparedQuery)
            : partnerConnection.query(preparedQuery);
        recordIndex = 0;
        noRecordsCreated = true;
        queryComplete = false;
      } catch (ConnectionException e) {
        throw new StageException(Errors.FORCE_08, e);
      }
    }

    SObject[] records = queryResult.getRecords();

    LOG.info("Retrieved {} records", records.length);

    if (recordCreator.isCountQuery()) {
      // Special case for old-style COUNT() query
      Record rec = getContext().createRecord(conf.soqlQuery);
      LinkedHashMap<String, Field> map = new LinkedHashMap<>();
      map.put(COUNT, Field.create(Field.Type.INTEGER, queryResult.getSize()));
      rec.set(Field.createListMap(map));
      rec.getHeader().setAttribute(ForceRecordCreator.SOBJECT_TYPE_ATTRIBUTE, sobjectType);
      noRecordsCreated = false;
      eventFired = false;

      batchMaker.addRecord(rec);
    } else {
      int endIndex = Math.min(recordIndex + maxBatchSize, records.length);

      for (; recordIndex < endIndex; recordIndex++) {
        SObject record = records[recordIndex];

        String offsetColumn = StringUtils.defaultIfEmpty(conf.offsetColumn, ID);
        XmlObject offsetField = getChildIgnoreCase(record, offsetColumn);
        String offset;
        if (offsetField == null || offsetField.getValue() == null) {
          if (!conf.disableValidation) {
            throw new StageException(Errors.FORCE_22, offsetColumn);
          }
          // Aggregate query - set the offset to the record index, since we have no offset column
          offset = String.valueOf(recordIndex);
        } else {
          offset = fixOffset(offsetColumn, offsetField.getValue().toString());
          nextSourceOffset = ForceUtils.RECORD_ID_OFFSET_PREFIX + offset;
        }
        final String sourceId = StringUtils.substring(conf.soqlQuery.replaceAll("[\n\r]", " "), 0, 100) + "::rowCount:" + recordIndex + (StringUtils.isEmpty(conf.offsetColumn) ? "" : ":" + offset);
        Record rec = recordCreator.createRecord(sourceId, record);
        noRecordsCreated = false;
        eventFired = false;
        noMoreDataRecordCount++;

        batchMaker.addRecord(rec);
      }
    }

    if (recordIndex == records.length) {
      try {
        if (queryResult.isDone()) {
          // We're out of results
          queryResult = null;
          lastQueryCompletedTime = System.currentTimeMillis();
          LOG.info("Query completed at: {}", lastQueryCompletedTime);
          queryComplete = true;
          if (conf.subscribeToStreaming) {
            // Switch to processing events
            nextSourceOffset = ForceUtils.READ_EVENTS_FROM_NOW;
          } else if (conf.repeatQuery == ForceRepeatQuery.FULL) {
            nextSourceOffset = ForceUtils.RECORD_ID_OFFSET_PREFIX + conf.initialOffset;
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

  // SDC-9078 - coerce scientific notation away when decimal field scale is zero
  // since Salesforce doesn't like scientific notation in queries
  private String fixOffset(String offsetColumn, String offset) {
    com.sforce.soap.partner.Field sfdcField = ((SobjectRecordCreator)recordCreator).getFieldMetadata(sobjectType, offsetColumn);
    if (SobjectRecordCreator.DECIMAL_TYPES.contains(sfdcField.getType().toString())
        && offset.contains("E")) {
      BigDecimal val = new BigDecimal(offset);
      offset = val.toPlainString();
      if (val.compareTo(MAX_OFFSET_INT) > 0 && !offset.contains(".")) {
        // We need the ".0" suffix since Salesforce doesn't like integer
        // bigger than 2147483647
        offset += ".0";
      }
    }
    return offset;
  }

  private void processMetaMessage(Message message, String nextSourceOffset) throws StageException {
    if (message.getChannel().startsWith(META_CONNECT)
        && NONE.equals(MapUtils.getString(message.getAdvice(), RECONNECT))) {
      // Need to restart the consumer after a disconnect when the CometD client doesn't do so
      forceConsumer.restart();
    } else if (message.getChannel().startsWith(META_HANDSHAKE) && message.isSuccessful()) {
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
      if (error == null) {
        error = message.get("failure").toString();
      }
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

    return ForceUtils.EVENT_ID_OFFSET_PREFIX + event.get(REPLAY_ID).toString();
  }

  private String streamingProduce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    String nextSourceOffset;
    if (getContext().isPreview()) {
      nextSourceOffset = ForceUtils.READ_EVENTS_FROM_START;
    } else if (null == lastSourceOffset) {
      if (conf.subscriptionType == SubscriptionType.PLATFORM_EVENT &&
          conf.replayOption == ReplayOption.ALL_EVENTS) {
        nextSourceOffset = ForceUtils.READ_EVENTS_FROM_START;
      } else {
        nextSourceOffset = ForceUtils.READ_EVENTS_FROM_NOW;
      }
    } else {
      nextSourceOffset = lastSourceOffset;
    }

    switch (conf.subscriptionType) {
      case PUSH_TOPIC:
        if (!(recordCreator instanceof PushTopicRecordCreator)) {
          recordCreator = new PushTopicRecordCreator((SobjectRecordCreator)recordCreator);
          recordCreator.init();
        }
        break;
      case CDC:
        if (!(recordCreator instanceof ChangeDataCaptureRecordCreator)) {
          recordCreator = new ChangeDataCaptureRecordCreator(getContext(), conf);
          recordCreator.init();
        }
        break;
    }

    if (recordCreator instanceof SobjectRecordCreator) {
      SobjectRecordCreator sobjectRecordCreator = (SobjectRecordCreator)recordCreator;
      if (!StringUtils.isEmpty(sobjectType) && !sobjectRecordCreator.metadataCacheExists()) {
        sobjectRecordCreator.buildMetadataCache(partnerConnection);
      }
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
}
