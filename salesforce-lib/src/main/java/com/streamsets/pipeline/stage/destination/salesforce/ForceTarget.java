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
package com.streamsets.pipeline.stage.destination.salesforce;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Multimap;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BulkConnection;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.SessionRenewer;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.cache.CacheCleaner;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.salesforce.ForceTargetConfigBean;
import com.streamsets.pipeline.lib.salesforce.ForceUtils;
import com.streamsets.pipeline.lib.salesforce.Errors;
import com.streamsets.pipeline.lib.salesforce.ForceFieldMapping;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.namespace.QName;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * This target writes records to Salesforce objects
 */
public class ForceTarget extends BaseTarget {

  private static final Logger LOG = LoggerFactory.getLogger(ForceTarget.class);
  private static final String SOBJECT_NAME = "sObjectNameTemplate";
  private static final String EXTERNAL_ID_NAME = "externalIdField";
  private static final String CONF_PREFIX = "conf";
  private ErrorRecordHandler errorRecordHandler;

  public final ForceTargetConfigBean conf;
  public ELVars externalIdFieldVars;
  public ELEval externalIdFieldEval;

  private final boolean useCompression;
  private final boolean showTrace;

  private SortedMap<String, String> customMappings;
  private PartnerConnection partnerConnection;
  private BulkConnection bulkConnection;
  private ELVars sObjectNameVars;
  private ELEval sObjectNameEval;

  // RelationshipName:IndexedFieldName
  private static final String BULK_LOADER_FIELD_SYNTAX = "^[a-zA-Z_]\\w*:[a-zA-Z_]\\w*$";
  private static final Pattern BULK_LOADER_FIELD_PATTERN = Pattern.compile(BULK_LOADER_FIELD_SYNTAX);

  private class ApiAndSObject {
    public boolean bulkApi;
    public String sObject;

    public ApiAndSObject(boolean bulkApi, String sObject) {
      this.bulkApi = bulkApi;
      this.sObject = sObject;
    }
  }

  private class ForceWriterLoader extends CacheLoader<ApiAndSObject, ForceWriter> {
    @Override
    public ForceWriter load(ApiAndSObject key) throws ConnectionException {
      if (key.bulkApi) {
        return new ForceBulkWriter(partnerConnection, key.sObject, customMappings, bulkConnection, getContext());
      } else {
        return new ForceSoapWriter(partnerConnection, key.sObject, customMappings);
      }
    }
  }

  protected LoadingCache<ApiAndSObject, ForceWriter> forceWriters;
  protected final CacheCleaner cacheCleaner;

  public ForceTarget(
      ForceTargetConfigBean conf, boolean useCompression, boolean showTrace
  ) {
    this.conf = conf;
    this.useCompression = useCompression;
    this.showTrace = showTrace;

    CacheBuilder cacheBuilder = CacheBuilder.newBuilder()
        .maximumSize(500)
        .expireAfterAccess(1, TimeUnit.HOURS);

    if(LOG.isDebugEnabled()) {
      cacheBuilder.recordStats();
    }

    forceWriters = cacheBuilder.build(new ForceWriterLoader());

    cacheCleaner = new CacheCleaner(forceWriters, "ForceTarget", 10 * 60 * 1000);
  }

  // Renew the Salesforce session on timeout
  private class ForceSessionRenewer implements SessionRenewer {
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

  /**
   * {@inheritDoc}
   */
  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = super.init();
    Target.Context context = getContext();
    Optional
        .ofNullable(conf.init(context, CONF_PREFIX))
        .ifPresent(issues::addAll);

    errorRecordHandler = new DefaultErrorRecordHandler(context);

    sObjectNameVars = getContext().createELVars();
    sObjectNameEval = context.createELEval(SOBJECT_NAME);
    ELUtils.validateExpression(conf.sObjectNameTemplate,
        context,
        Groups.FORCE.getLabel(),
        SOBJECT_NAME,
        Errors.FORCE_12, issues
    );

    externalIdFieldVars = getContext().createELVars();
    externalIdFieldEval = context.createELEval(EXTERNAL_ID_NAME);
    ELUtils.validateExpression(conf.externalIdField,
        context,
        Groups.FORCE.getLabel(),
        EXTERNAL_ID_NAME,
        Errors.FORCE_24, issues
    );

    if (issues.isEmpty()) {
      customMappings = new TreeMap<>();
      for (ForceFieldMapping mapping : conf.fieldMapping) {
        // SDC-7446 Allow colon as well as period as field separator
        // SDC-13117 But only if it's a Bulk Loader syntax field name
        String salesforceField = (conf.useBulkAPI && BULK_LOADER_FIELD_PATTERN.matcher(mapping.salesforceField).matches())
            ? mapping.salesforceField.replace(':', '.')
            : mapping.salesforceField;
        customMappings.put(salesforceField, mapping.sdcField);
      }

      try {
        ConnectorConfig partnerConfig = ForceUtils.getPartnerConfig(conf, new ForceSessionRenewer());
        partnerConnection = Connector.newConnection(partnerConfig);
        if (conf.connection.mutualAuth.useMutualAuth) {
          ForceUtils.setupMutualAuth(partnerConfig, conf.connection.mutualAuth);
        }
        bulkConnection = ForceUtils.getBulkConnection(partnerConfig, conf);
        LOG.info("Successfully authenticated as {}", conf.connection.username);
      } catch (ConnectionException | AsyncApiException | StageException | URISyntaxException ce) {
        LOG.error("Can't connect to SalesForce", ce);
        issues.add(getContext().createConfigIssue(Groups.FORCE.name(),
            "connectorConfig",
            Errors.FORCE_00,
            ForceUtils.getExceptionCode(ce) + ", " + ForceUtils.getExceptionMessage(ce)
        ));
      }
    }

    // If issues is not empty, the UI will inform the user of each configuration issue in the list.
    return issues;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void destroy() {
    super.destroy();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(Batch batch) throws StageException {
    if (!batch.getRecords().hasNext()) {
      // No records - take the opportunity to clean up the cache so that we don't hold on to memory indefinitely
      cacheCleaner.periodicCleanUp();
    }

    Multimap<String, Record> partitions = ELUtils.partitionBatchByExpression(sObjectNameEval,
        sObjectNameVars,
        conf.sObjectNameTemplate,
        batch
    );
    Set<String> sObjectNames = partitions.keySet();
    for (String sObjectName : sObjectNames) {
      ForceWriter writer = forceWriters.getUnchecked(new ApiAndSObject(conf.useBulkAPI, sObjectName));
      List<OnRecordErrorException> errors = writer.writeBatch(
          sObjectName,
          partitions.get(sObjectName),
          this
      );
      for (OnRecordErrorException error : errors) {
        errorRecordHandler.onError(error);
      }
    }
  }
}
