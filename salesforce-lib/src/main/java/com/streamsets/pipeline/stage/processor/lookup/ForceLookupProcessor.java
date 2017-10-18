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

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.SessionRenewer;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.cache.CacheCleaner;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.salesforce.DataType;
import com.streamsets.pipeline.lib.salesforce.Errors;
import com.streamsets.pipeline.lib.salesforce.ForceLookupConfigBean;
import com.streamsets.pipeline.lib.salesforce.ForceSDCFieldMapping;
import com.streamsets.pipeline.lib.salesforce.ForceUtils;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.origin.salesforce.Groups;
import com.streamsets.pipeline.stage.processor.kv.EvictionPolicyType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.namespace.QName;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ForceLookupProcessor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(ForceLookupProcessor.class);
  final ForceLookupConfigBean conf;

  Map<String, String> columnsToFields = new HashMap<>();
  Map<String, String> columnsToDefaults = new HashMap<>();
  Map<String, DataType> columnsToTypes = new HashMap<>();

  private LoadingCache<String, Map<String, Field>> cache;

  PartnerConnection partnerConnection;
  private ErrorRecordHandler errorRecordHandler;
  private ELEval queryEval;
  Map<String, Map<String, com.sforce.soap.partner.Field>> metadataMap;
  private CacheCleaner cacheCleaner;

  public ForceLookupProcessor(ForceLookupConfigBean conf) {
    this.conf = conf;
  }

  // Renew the Salesforce session on timeout
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

    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    queryEval = getContext().createELEval("soqlQuery");

    if (issues.isEmpty()) {
      try {
        ConnectorConfig partnerConfig = ForceUtils.getPartnerConfig(conf, new ForceLookupProcessor.ForceSessionRenewer());

        partnerConnection = new PartnerConnection(partnerConfig);
      } catch (ConnectionException | StageException e) {
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
    }

    return issues;
  }

  @Override
  public void process(Batch batch, SingleLaneBatchMaker batchMaker) throws StageException {
    if (batch.getRecords().hasNext()) {
      // New metadata map for each batch
      metadataMap = new LinkedHashMap<>();
    } else {
      // No records - take the opportunity to clean up the cache so that we don't hold on to memory indefinitely
      cacheCleaner.periodicCleanUp();
    }
    super.process(batch, batchMaker);
  }

  @Override
  protected void process(
      Record record, SingleLaneBatchMaker batchMaker
  ) throws StageException {
    try {
      ELVars elVars = getContext().createELVars();
      RecordEL.setRecordInContext(elVars, record);
      String preparedQuery = prepareQuery(conf.soqlQuery, elVars);
      Map<String, Field> fieldMap = cache.get(preparedQuery);
      if (fieldMap.isEmpty()) {
        // No results
        LOG.error(Errors.FORCE_15.getMessage(), preparedQuery);
        errorRecordHandler.onError(new OnRecordErrorException(record, Errors.FORCE_15, preparedQuery));
      }
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
              Map<String, Field> cell = new HashMap<>();
              cell.put("header", Field.create(columnName));
              cell.put("value", field);
              field = Field.create(cell);
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
      batchMaker.addRecord(record);
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

  private String prepareQuery(String soqlQuery, ELVars elVars) throws StageException {
    String preparedQuery = queryEval.eval(elVars, soqlQuery, String.class);
    String sobjectType = ForceUtils.getSobjectTypeFromQuery(preparedQuery);

    if (metadataMap.get(sobjectType.toLowerCase()) == null) {
      LOG.debug("Getting metadata for sobjectType {} - query is {}", sobjectType, preparedQuery);
      try {
        ForceUtils.getAllReferences(partnerConnection, metadataMap, new String[]{sobjectType}, ForceUtils.METADATA_DEPTH);
      } catch (ConnectionException e) {
        throw new StageException(Errors.FORCE_21, sobjectType, e);
      }
    }

    preparedQuery = ForceUtils.expandWildcard(preparedQuery, sobjectType, metadataMap);

    return preparedQuery;
  }

  @SuppressWarnings("unchecked")
  private LoadingCache<String, Map<String, Field>> buildCache() {
    CacheBuilder cacheBuilder = CacheBuilder.newBuilder();
    if (!conf.cacheConfig.enabled) {
      return cacheBuilder.maximumSize(0).build(new ForceLookupLoader(this));
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

    LoadingCache<String, Map<String, Field>> cache = cacheBuilder.build(new ForceLookupLoader(this));

    return cache;
  }
}
