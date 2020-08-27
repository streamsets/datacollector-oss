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
package com.streamsets.pipeline.stage.processor.lookup;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BulkConnection;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.soap.partner.fault.InvalidFieldFault;
import com.sforce.soap.partner.fault.InvalidIdFault;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.SessionRenewer;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ToErrorContext;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.cache.CacheCleaner;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.salesforce.BulkRecordCreator;
import com.streamsets.pipeline.lib.salesforce.DataType;
import com.streamsets.pipeline.lib.salesforce.Errors;
import com.streamsets.pipeline.lib.salesforce.ForceLookupConfigBean;
import com.streamsets.pipeline.lib.salesforce.ForceRecordCreator;
import com.streamsets.pipeline.lib.salesforce.ForceSDCFieldMapping;
import com.streamsets.pipeline.lib.salesforce.ForceStage;
import com.streamsets.pipeline.lib.salesforce.ForceUtils;
import com.streamsets.pipeline.lib.salesforce.LookupMode;
import com.streamsets.pipeline.lib.salesforce.SoapRecordCreator;
import com.streamsets.pipeline.lib.salesforce.SobjectRecordCreator;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.origin.salesforce.Groups;
import com.streamsets.pipeline.stage.processor.kv.EvictionPolicyType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.namespace.QName;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.streamsets.pipeline.lib.salesforce.LookupMode.QUERY;

public class ForceLookupProcessor extends SingleLaneRecordProcessor implements ForceStage {
  // Defined by Salesforce SOAP API
  private static final int MAX_OBJECT_IDS = 2000;
  private static final Logger LOG = LoggerFactory.getLogger(ForceLookupProcessor.class);
  private final ForceLookupConfigBean conf;
  private static final String CONF_PREFIX = "conf";

  private Map<String, String> columnsToFields = new HashMap<>();
  private Map<String, String> columnsToDefaults = new HashMap<>();
  Map<String, DataType> columnsToTypes = new HashMap<>();

  private Cache<String, Optional<List<Map<String, Field>>>> cache;
  private AtomicBoolean destroyed = new AtomicBoolean(false);

  PartnerConnection partnerConnection;
  private BulkConnection bulkConnection;
  private ErrorRecordHandler errorRecordHandler;
  private ELEval queryEval;
  private CacheCleaner cacheCleaner;
  private SobjectRecordCreator recordCreator;

  public ForceLookupProcessor(ForceLookupConfigBean conf) {
    this.conf = conf;
  }

  public BulkConnection getBulkConnection() {
    return bulkConnection;
  }

  public ForceRecordCreator getRecordCreator() {
    return recordCreator;
  }

  public ForceLookupConfigBean getConfig() {
    return conf;
  }

  // Renew the Salesforce session on timeout
  @SuppressWarnings("Duplicates")
  public class ForceSessionRenewer implements SessionRenewer {
    @Override
    public SessionRenewalHeader renewSession(ConnectorConfig config) throws ConnectionException {
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
    List<ConfigIssue> issues = super.init();
    Optional
        .ofNullable(conf.init(getContext(), CONF_PREFIX ))
        .ifPresent(issues::addAll);

    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    queryEval = getContext().createELEval("soqlQuery");

    if (issues.isEmpty()) {
      try {
        ConnectorConfig partnerConfig = ForceUtils.getPartnerConfig(conf, new ForceLookupProcessor.ForceSessionRenewer());

        partnerConnection = new PartnerConnection(partnerConfig);
        if (conf.connection.mutualAuth.useMutualAuth) {
          ForceUtils.setupMutualAuth(partnerConfig, conf.connection.mutualAuth);
        }
        if (conf.useBulkAPI) {
          bulkConnection = ForceUtils.getBulkConnection(partnerConfig, conf);
        }
      } catch (ConnectionException | StageException | URISyntaxException | AsyncApiException e) {
        LOG.error("Error connecting: {}", e);
        issues.add(getContext().createConfigIssue(Groups.FORCE.name(),
            "connectorConfig",
            Errors.FORCE_00,
            ForceUtils.getExceptionCode(e) + ", " + ForceUtils.getExceptionMessage(e)
        ));
      }
    }

    for (ForceSDCFieldMapping mapping : conf.fieldMappings) {
      LOG.debug("Mapping Salesforce field {} to SDC field {}", mapping.salesforceField, mapping.sdcField);
      columnsToFields.put(mapping.salesforceField.toLowerCase(), mapping.sdcField);
      if (!StringUtils.isEmpty(mapping.defaultValue) && mapping.dataType == DataType.USE_SALESFORCE_TYPE) {
        issues.add(getContext().createConfigIssue(Groups.FORCE.name(),
            "fieldMappings",
            Errors.FORCE_18,
            mapping.salesforceField)
        );
      }
      columnsToDefaults.put(mapping.salesforceField.toLowerCase(), mapping.defaultValue);
      columnsToTypes.put(mapping.salesforceField.toLowerCase(), mapping.dataType);
    }

    if (issues.isEmpty()) {
      cache = buildCache();
      cacheCleaner = new CacheCleaner(cache, "ForceLookupProcessor", 10 * 60 * 1000);
      if (conf.lookupMode == LookupMode.RETRIEVE) {
        // All records are of the configured object type, so we only need one record creator
        recordCreator = new SoapRecordCreator(getContext(), conf, conf.sObjectType.toLowerCase());
      }
    }

    return issues;
  }

  @Override
  public void process(Batch batch, SingleLaneBatchMaker batchMaker) throws StageException {
    long start = System.currentTimeMillis();
    if (conf.lookupMode == QUERY) {
      processQuery(batch, batchMaker);
    } else {
      processRetrieve(batch, batchMaker);
    }
    LOG.debug("Salesforce lookup batch took {} milliseconds in {} mode",
        System.currentTimeMillis() - start,
        conf.lookupMode.getLabel());
  }

  private void processRetrieve(Batch batch, SingleLaneBatchMaker batchMaker) throws StageException {
    Iterator<Record> it = batch.getRecords();

    if (!it.hasNext()) {
      emptyBatch(batchMaker);
      return;
    }

    // New metadata cache for each batch
    recordCreator.buildMetadataCacheFromFieldList(partnerConnection, conf.retrieveFields);

    // Could be more than one record with the same value in the lookup
    // field, so we have to build a multimap
    ListMultimap<String, Record> recordsToRetrieve = LinkedListMultimap.create();

    // Iterate through records - three cases
    // * no ID field => use default field values
    // * ID in cache => use cached field values
    // * otherwise   => add ID to list for retrieval
    while (it.hasNext()) {
      Record record = it.next();
      Field idField = record.get(conf.idField);
      String id = (idField != null) ? idField.getValueAsString() : null;
      if (Strings.isNullOrEmpty(id)) {
        switch (conf.missingValuesBehavior) {
          case SEND_TO_ERROR:
            LOG.error(Errors.FORCE_35.getMessage());
            errorRecordHandler.onError(new OnRecordErrorException(record, Errors.FORCE_35));
            break;
          case PASS_RECORD_ON:
            setFieldsInRecord(record, getDefaultFields());
            break;
          default:
            throw new IllegalStateException("Unknown missing value behavior: " + conf.missingValuesBehavior);
        }
      } else {
        Optional<List<Map<String, Field>>> entry = cache.getIfPresent(id);

        if (entry != null && entry.isPresent()) {
          // Salesforce record id is unique, so we'll always have just one entry in the list
          setFieldsInRecord(record, entry.get().get(0));
        } else {
          recordsToRetrieve.put(id, record);
        }
      }
    }

    Set<Record> badRecords = new HashSet<>();
    if (!recordsToRetrieve.isEmpty()) {
      String fieldList = ("*".equals(conf.retrieveFields.trim()))
          ? recordCreator.expandWildcard()
          : conf.retrieveFields;
      String[] idArray = recordsToRetrieve.keySet().toArray(new String[0]);

      // Split batch into 'chunks'
      int start = 0;
      while (start < idArray.length) {
        int end = start + Math.min(MAX_OBJECT_IDS, idArray.length - start);
        String[] ids = Arrays.copyOfRange(idArray, start, end);
        try {
          SObject[] sObjects = partnerConnection.retrieve(fieldList,
              conf.sObjectType,
              ids
          );

          for (int i = 0; i < sObjects.length; i++) {
            SObject sObject = sObjects[i];
            if (sObject == null) {
              // No results for this id - probably deleted
              switch (conf.missingValuesBehavior) {
                case SEND_TO_ERROR:
                  LOG.error(Errors.FORCE_45.getMessage(), ids[i]);
                  List<Record> errorRecords = recordsToRetrieve.get(ids[i]);
                  for (Record record: errorRecords) {
                    errorRecordHandler.onError(new OnRecordErrorException(record, Errors.FORCE_45, ids[i]));
                  }
                  badRecords.addAll(errorRecords);
                  break;
                case PASS_RECORD_ON:
                  for (Record record: recordsToRetrieve.get(ids[i])) {
                    setFieldsInRecord(record, getDefaultFields());
                  }
                  break;
                default:
                  throw new IllegalStateException("Unknown missing value behavior: " + conf.missingValuesBehavior);
              }
            } else {
              String id = sObject.getId();
              // TODO - ugly cast
              Map<String, Field> fieldMap = ((SoapRecordCreator)recordCreator).addFields(sObject, columnsToTypes);
              for (Record record : recordsToRetrieve.get(id)) {
                setFieldsInRecord(record, fieldMap);
              }
              cache.put(id, Optional.of(ImmutableList.of(fieldMap)));
            }
          }
        } catch (InvalidIdFault e) {
          // exceptionMessage has form "malformed id 0013600001NnbAdOnE"
          String badId = e.getExceptionMessage().split(" ")[2];
          LOG.error("Bad Salesforce ID: {}", badId);
          switch (getContext().getOnErrorRecord()) {
            case DISCARD:
              // Need to discard whole chunk!
              addRecordsToSet(ids, recordsToRetrieve, badRecords);
              break;
            case TO_ERROR:
              // Need to send the entire chunk to error - none of them were processed!
              sendChunkToError(ids, recordsToRetrieve, getContext(), e);
              addRecordsToSet(ids, recordsToRetrieve, badRecords);
              break;
            case STOP_PIPELINE:
              Record badRecord = recordsToRetrieve.get(badId).get(0);
              throw new OnRecordErrorException(badRecord, Errors.FORCE_29, badId, e);
            default:
              throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
                  getContext().getOnErrorRecord(),
                  e
              ));
          }
        } catch (InvalidFieldFault e) {
          switch (getContext().getOnErrorRecord()) {
            case DISCARD:
              // Need to discard whole chunk!
              addRecordsToSet(ids, recordsToRetrieve, badRecords);
              break;
            case TO_ERROR:
              // Need to send the entire chunk to error - none of them were processed!
              sendChunkToError(ids, recordsToRetrieve, getContext(), e);
              addRecordsToSet(ids, recordsToRetrieve, badRecords);
              break;
            case STOP_PIPELINE:
              throw new StageException(Errors.FORCE_30, e.getExceptionMessage(), e);
            default:
              throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
                  getContext().getOnErrorRecord(),
                  e
              ));
          }
        } catch (ConnectionException e) {
          throw new StageException(Errors.FORCE_28, e.getMessage(), e);
        }
        start = end;
      }
    }

    it = batch.getRecords();
    while (it.hasNext()) {
      Record record = it.next();
      if (!badRecords.contains(record)) {
        batchMaker.addRecord(record);
      }
    }
  }

  private void addRecordsToSet(String[] ids, ListMultimap<String, Record> recordMultimap, Set<Record> recordSet) {
    for (String id : ids) {
      recordSet.addAll(recordMultimap.get(id));
    }
  }

  private static void sendChunkToError(
      String[] ids,
      ListMultimap<String, Record> recordsToRetrieve,
      ToErrorContext context,
      ApiFault e
  ) {
    for (String id : ids) {
      for (Record record : recordsToRetrieve.get(id)) {
        context.toError(record, e);
      }
    }
  }

  Map<String, Field> getDefaultFields() throws OnRecordErrorException {
    Map<String, Field> fieldMap = new HashMap<>();
    for (String key : columnsToFields.keySet()) {
      String val = columnsToDefaults.get(key);
      try {
        if (columnsToTypes.get(key) != DataType.USE_SALESFORCE_TYPE) {
          Field field = Field.create(Field.Type.valueOf(columnsToTypes.get(key).getLabel()), val);
          fieldMap.put(key, field);
        }
      } catch (IllegalArgumentException e) {
        throw new OnRecordErrorException(Errors.FORCE_20, key, val, e);
      }
    }
    return fieldMap;
  }

  private void setFieldsInRecord(Record record, Map<String, Field> fieldMap) {
    for (Map.Entry<String, Field> entry : fieldMap.entrySet()) {
      String columnName = entry.getKey();
      String fieldPath = columnsToFields.get(columnName.toLowerCase());
      Field field = entry.getValue();
      if (fieldPath == null) {
        Field root = record.get();
        // No mapping
        switch (root.getType()) {
          case LIST:
            // Add new field to the end of the list
            fieldPath = "[" + root.getValueAsList().size() + "]";
            field = Field.create(ImmutableMap.of(
                "header", Field.create(columnName),
                "value", field));
            break;
          case LIST_MAP:
          case MAP:
            // Just use the column name
            fieldPath = "/" + columnName;
            break;
          default:
            break;
        }
      }
      record.set(fieldPath, field);
    }
  }

  private void processQuery(Batch batch, SingleLaneBatchMaker batchMaker) throws StageException {
    if (batch.getRecords().hasNext()) {
      // New record creator for each batch
      if (recordCreator != null) {
        recordCreator.clearMetadataCache();
      }
    } else {
      // No records - take the opportunity to clean up the cache so that we don't hold on to memory indefinitely
      cacheCleaner.periodicCleanUp();
    }
    super.process(batch, batchMaker);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void process(
      Record record, SingleLaneBatchMaker batchMaker
  ) throws StageException {
    try {
      ELVars elVars = getContext().createELVars();
      RecordEL.setRecordInContext(elVars, record);
      TimeNowEL.setTimeNowInContext(elVars, new Date());
      String preparedQuery = prepareQuery(queryEval.eval(elVars, conf.soqlQuery, String.class));
      // Need this ugly cast since there isn't a way to do a simple
      // get with the Cache interface
      LOG.debug("Prepared query is {}", preparedQuery);
      Optional<List<Map<String, Field>>> entry = ((LoadingCache<String, Optional<List<Map<String, Field>>>>)cache).get(preparedQuery);
      LOG.debug("entry {}", entry.isPresent() ? entry.get() : null);

      if (!entry.isPresent()) {
        // No results
        switch (conf.missingValuesBehavior) {
          case SEND_TO_ERROR:
            LOG.error(Errors.FORCE_15.getMessage(), preparedQuery);
            errorRecordHandler.onError(new OnRecordErrorException(record, Errors.FORCE_15, preparedQuery));
            break;
          case PASS_RECORD_ON:
            batchMaker.addRecord(record);
            break;
          default:
            throw new IllegalStateException("Unknown missing value behavior: " + conf.missingValuesBehavior);
        }
      } else {
        List<Map<String, Field>> values = entry.get();
        switch (conf.multipleValuesBehavior) {
          case FIRST_ONLY:
            setFieldsInRecord(record, values.get(0));
            batchMaker.addRecord(record);
            break;
          case SPLIT_INTO_MULTIPLE_RECORDS:
            int i = 0;
            for (Map<String, Field> lookupItem : values) {
              Record newRecord = getContext().cloneRecord(record, String.valueOf(i++));
              setFieldsInRecord(newRecord, lookupItem);
              batchMaker.addRecord(newRecord);
            }
            break;
          case ALL_AS_LIST:
            Map<String, List<Field>> valuesMap = new HashMap<>();
            for (Map<String, Field> lookupItem : values) {
              lookupItem.forEach((k, v) -> {
                if (valuesMap.get(k) == null) {
                  List<Field> lookupValue = new ArrayList<>();
                  valuesMap.put(k, lookupValue);
                }
                valuesMap.get(k).add(v);
              });
            }
            Map<String, Field> valueMap = new HashMap<>();
            valuesMap.forEach( (k,v) -> valueMap.put(k, Field.create(v)));
            setFieldsInRecord(record, valueMap);
            batchMaker.addRecord(record);
            break;
          default:
            throw new IllegalStateException("Unknown multiple value behavior: " + conf.multipleValuesBehavior);
        }
      }
    } catch (ELEvalException e) {
      LOG.error(Errors.FORCE_16.getMessage(), conf.soqlQuery, e);
      throw new OnRecordErrorException(record, Errors.FORCE_16, conf.soqlQuery);
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), StageException.class);
      throw new IllegalStateException(e); // The cache loader shouldn't throw anything that isn't a StageException.
    } catch (OnRecordErrorException error) { // NOSONAR
      errorRecordHandler.onError(new OnRecordErrorException(record, error.getErrorCode(), error.getParams()));
    }

  }

  private SobjectRecordCreator buildRecordCreator(String sobjectType) {
    if (conf.useBulkAPI) {
      return new BulkRecordCreator(getContext(), conf, sobjectType);
    }
    return new SoapRecordCreator(getContext(), conf, sobjectType);
  }

  private String prepareQuery(String preparedQuery) throws StageException {
    String sobjectType = ForceUtils.getSobjectTypeFromQuery(preparedQuery);

    if (recordCreator == null || ! sobjectType.equals(recordCreator.getSobjectType())) {
      recordCreator = buildRecordCreator(sobjectType);
    }

    if (recordCreator.queryHasWildcard(preparedQuery)) {
      if (!recordCreator.metadataCacheExists()) {
        // No need to follow relationships on a wildcard query, so build the cache from the object type
        recordCreator.buildMetadataCache(partnerConnection);
      }
      preparedQuery = recordCreator.expandWildcard(preparedQuery);
    } else {
      if (!recordCreator.metadataCacheExists()) {
        recordCreator.buildMetadataCacheFromQuery(partnerConnection, preparedQuery);
      }
    }

    return preparedQuery;
  }

  @SuppressWarnings("unchecked")
  private Cache<String, Optional<List<Map<String, Field>>>> buildCache() {
    CacheBuilder cacheBuilder = CacheBuilder.newBuilder();

    if (!conf.cacheConfig.enabled) {
      return (conf.lookupMode == QUERY)
          ? cacheBuilder.build(conf.useBulkAPI ? new ForceLookupBulkLoader(this) : new ForceLookupSoapLoader(this))
          : cacheBuilder.maximumSize(0).build();
    }

    if (conf.cacheConfig.maxSize == -1) {
      conf.cacheConfig.maxSize = Long.MAX_VALUE;
    }

    if(LOG.isDebugEnabled()) {
      cacheBuilder.recordStats();
    }

    // CacheBuilder doesn't support specifying type thus suffers from erasure, so
    // we build it with this if / else logic.
    if (conf.cacheConfig.evictionPolicyType == EvictionPolicyType.EXPIRE_AFTER_ACCESS) {
      cacheBuilder.maximumSize(conf.cacheConfig.maxSize).expireAfterAccess(conf.cacheConfig.expirationTime, conf.cacheConfig.timeUnit);
    } else if (conf.cacheConfig.evictionPolicyType == EvictionPolicyType.EXPIRE_AFTER_WRITE) {
      cacheBuilder.maximumSize(conf.cacheConfig.maxSize).expireAfterWrite(conf.cacheConfig.expirationTime, conf.cacheConfig.timeUnit);
    } else {
      throw new IllegalArgumentException(Utils.format("Unrecognized EvictionPolicyType: '{}'",
          conf.cacheConfig.evictionPolicyType
      ));
    }

    return (conf.lookupMode == QUERY)
        ? cacheBuilder.build(conf.useBulkAPI ? new ForceLookupBulkLoader(this) : new ForceLookupSoapLoader(this))
        : cacheBuilder.build();
  }

  public boolean isDestroyed() {
    return destroyed.get();
  }

  @Override
  public void destroy() {
    destroyed.set(true);

    super.destroy();
  }
}
