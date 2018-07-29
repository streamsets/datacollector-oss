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
package com.streamsets.pipeline.stage.destination.hive;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.streamsets.datacollector.security.HadoopSecurityUtil;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.api.lineage.EndPointType;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.cache.CacheCleaner;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.lib.hive.Errors;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.RecordWriter;
import org.apache.hive.hcatalog.streaming.StreamingConnection;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.hive.hcatalog.streaming.StrictJsonWriter;
import org.apache.hive.hcatalog.streaming.TransactionBatch;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Writes to Hive using Hive's Streaming API.
 * Currently tables must be backed by ORC file to use this API.
 */
public class HiveTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(HiveTarget.class);
  private static final String SDC_FIELD_SEP = "/";
  private static final String HIVE_METASTORE_URI = "hive.metastore.uris";

  private final String hiveThriftUrl;
  private final String schema;
  private final String tableName;
  private final String hiveConfDir;
  private final boolean autoCreatePartitions;
  private final int txnBatchSize;
  private final int bufferLimit;
  private final List<FieldMappingConfig> columnMappings;
  private final Map<String, String> additionalHiveProperties;

  private Map<String, String> columnsToFields;
  private Map<String, String> partitionsToFields;
  private HiveConf hiveConf;
  private UserGroupInformation loginUgi;
  private ErrorRecordHandler errorRecordHandler;
  private DataGeneratorFactory dataGeneratorFactory;

  private LoadingCache<HiveEndPoint, StreamingConnection> hiveConnectionPool;
  private LoadingCache<HiveEndPoint, RecordWriter> recordWriterPool;
  private CacheCleaner hiveConnectionCacheCleaner;
  private CacheCleaner recordWriterCacheCleaner;

  class HiveConnectionLoader extends CacheLoader<HiveEndPoint, StreamingConnection> {
    @Override
    public StreamingConnection load(HiveEndPoint endPoint) throws StageException {
      StreamingConnection connection;
      try {
         connection = endPoint.newConnection(autoCreatePartitions, hiveConf, loginUgi);
      } catch (StreamingException | InterruptedException e) {
        throw new StageException(Errors.HIVE_09, e.toString(), e);
      }
      return connection;
    }
  }

  static class HiveConnectionRemovalListener implements RemovalListener<HiveEndPoint, StreamingConnection> {
    @Override
    public void onRemoval(RemovalNotification<HiveEndPoint, StreamingConnection> notification) {
      LOG.debug("Evicting StreamingConnection from pool: {}", notification);
      StreamingConnection connection = notification.getValue();
      if (null != connection) {
        connection.close();
      }
    }
  }

  class HiveRecordWriterLoader extends CacheLoader<HiveEndPoint, RecordWriter> {
    @Override
    public RecordWriter load(HiveEndPoint endPoint) throws Exception {
      return new StrictJsonWriter(endPoint, hiveConf);
    }
  }

  public HiveTarget(
      String hiveThriftUrl,
      String schema,
      String tableName,
      String hiveConfDir,
      List<FieldMappingConfig> columnMappings,
      boolean autoCreatePartitions,
      int txnBatchSize,
      int bufferLimitKb,
      Map<String, String> additionalHiveProperties
  ) {
    this.hiveThriftUrl = hiveThriftUrl;
    this.schema = schema;
    this.tableName = tableName;
    this.hiveConfDir = hiveConfDir;
    this.columnMappings = columnMappings;
    this.autoCreatePartitions = autoCreatePartitions;
    this.txnBatchSize = txnBatchSize;
    this.additionalHiveProperties = additionalHiveProperties;
    bufferLimit = 1000 * bufferLimitKb;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    partitionsToFields = new HashMap<>();
    columnsToFields = new HashMap<>();

    hiveConf = new HiveConf();

    // either the hiveConfDir should be set and valid, or the metastore URL must be set. (it's possible both are true!)
    if (null != hiveConfDir && !hiveConfDir.isEmpty()) {
      initHiveConfDir(issues);
    } else if (hiveThriftUrl == null || hiveThriftUrl.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.HIVE.name(), "hiveThriftUrl", Errors.HIVE_13));
    }

    // Specified URL overrides what's in the Hive Conf iff it's present
    if (hiveThriftUrl != null && !hiveThriftUrl.isEmpty()) {
      hiveConf.set(HIVE_METASTORE_URI, hiveThriftUrl);
    }

    if (!validateHiveThriftUrl(issues)) {
      return issues;
    }

    // Add any additional hive conf overrides
    for (Map.Entry<String, String> entry : additionalHiveProperties.entrySet()) {
      hiveConf.set(entry.getKey(), entry.getValue());
    }

    captureLoginUGI(issues);
    initHiveMetaStoreClient(issues);
    applyCustomMappings(issues);

    dataGeneratorFactory = createDataGeneratorFactory();

    // Note that cleanup is done synchronously by default while servicing .get
    hiveConnectionPool = CacheBuilder.newBuilder()
        .maximumSize(10)
        .expireAfterAccess(10, TimeUnit.MINUTES)
        .removalListener(new HiveConnectionRemovalListener())
        .build(new HiveConnectionLoader());

    hiveConnectionCacheCleaner = new CacheCleaner(hiveConnectionPool, "HiveTarget connection pool", 10 * 60 * 1000);

    recordWriterPool = CacheBuilder.newBuilder()
        .maximumSize(10)
        .expireAfterAccess(10, TimeUnit.MINUTES)
        .build(new HiveRecordWriterLoader());

    recordWriterCacheCleaner = new CacheCleaner(recordWriterPool, "HiveTarget record writer pool", 10 * 60 * 1000);

    LineageEvent event = getContext().createLineageEvent(LineageEventType.ENTITY_WRITTEN);
    if(hiveThriftUrl != null && !hiveThriftUrl.isEmpty()) {
      event.setSpecificAttribute(LineageSpecificAttribute.DESCRIPTION, hiveThriftUrl);
    } else {
      event.setSpecificAttribute(LineageSpecificAttribute.DESCRIPTION,hiveConfDir);
    }
    event.setSpecificAttribute(LineageSpecificAttribute.ENDPOINT_TYPE, EndPointType.HIVE.name());
    event.setSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME, schema + " " + tableName);
    getContext().publishLineageEvent(event);

    LOG.debug("Total issues: {}", issues.size());
    return issues;
  }

  private void applyCustomMappings(List<ConfigIssue> issues) {
    // Now apply any custom mappings
    if (validColumnMappings(issues)) {
      for (FieldMappingConfig mapping : columnMappings) {
        LOG.debug("Custom mapping field {} to column {}", mapping.field, mapping.columnName);
        if (columnsToFields.containsKey(mapping.columnName)) {
          LOG.debug("Mapping field {} to column {}", mapping.field, mapping.columnName);
          columnsToFields.put(mapping.columnName, mapping.field);
        } else if (partitionsToFields.containsKey(mapping.columnName)) {
          LOG.debug("Mapping field {} to partition {}", mapping.field, mapping.columnName);
          partitionsToFields.put(mapping.columnName, mapping.field);
        }
      }
    }
  }

  private void captureLoginUGI(List<ConfigIssue> issues) {
    try {
      // forcing UGI to initialize with the security settings from the stage
      loginUgi = HadoopSecurityUtil.getLoginUser(hiveConf);
      // Proxy users are not currently supported due to: https://issues.apache.org/jira/browse/HIVE-11089
    } catch (Exception e) {
      issues.add(getContext().createConfigIssue(Groups.HIVE.name(), null, Errors.HIVE_11, e.getMessage()));
    }
  }

  private void initHiveMetaStoreClient(List<ConfigIssue> issues) {
    try {
      issues.addAll(loginUgi.doAs(
          new PrivilegedExceptionAction<List<ConfigIssue>>() {
            @Override
            public List<ConfigIssue> run() {
              List<ConfigIssue> issues = new ArrayList<>();
              HiveMetaStoreClient client = null;
              try {
                client = new HiveMetaStoreClient(hiveConf);

                List<FieldSchema> columnNames = client.getFields(schema, tableName);
                for (FieldSchema field : columnNames) {
                  columnsToFields.put(field.getName(), SDC_FIELD_SEP + field.getName());
                }

                Table table = client.getTable(schema, tableName);
                List<FieldSchema> partitionKeys = table.getPartitionKeys();
                for (FieldSchema field : partitionKeys) {
                  partitionsToFields.put(field.getName(), SDC_FIELD_SEP + field.getName());
                }
              } catch (UnknownDBException e) {
                issues.add(getContext().createConfigIssue(Groups.HIVE.name(), "schema", Errors.HIVE_02, schema));
              } catch (UnknownTableException e) {
                issues.add(
                    getContext().createConfigIssue(Groups.HIVE.name(), "table", Errors.HIVE_03, schema, tableName)
                );
              } catch (MetaException e) {
                issues.add(
                    getContext().createConfigIssue(Groups.HIVE.name(), "hiveUrl", Errors.HIVE_05, e.getMessage())
                );
              } catch (TException e) {
                issues.add(
                    getContext().createConfigIssue(Groups.HIVE.name(), "hiveUrl", Errors.HIVE_04, e.getMessage())
                );
              } finally {
                if (null != client) {
                  client.close();
                }
              }
              return issues;
            }
          }
      ));
    } catch (Error | IOException | InterruptedException e) {
      LOG.error("Received unknown error in validation: {}", e.toString(), e);
      issues.add(getContext().createConfigIssue(Groups.HIVE.name(), "", Errors.HIVE_01, e.toString()));
    } catch (UndeclaredThrowableException e) {
      LOG.error("Received unknown error in validation: {}", e.toString(), e);
      issues.add(
          getContext().createConfigIssue(Groups.HIVE.name(), "", Errors.HIVE_01, e.getUndeclaredThrowable().toString())
      );
    }
  }

  private void initHiveConfDir(List<ConfigIssue> issues) {
    File hiveConfDir = new File(this.hiveConfDir);

    if (!hiveConfDir.isAbsolute()) {
      hiveConfDir = new File(getContext().getResourcesDirectory(), this.hiveConfDir).getAbsoluteFile();
    }

    if (hiveConfDir.exists()) {
      File coreSite = new File(hiveConfDir.getAbsolutePath(), "core-site.xml");
      File hiveSite = new File(hiveConfDir.getAbsolutePath(), "hive-site.xml");
      File hdfsSite = new File(hiveConfDir.getAbsolutePath(), "hdfs-site.xml");

      if (!coreSite.exists()) {
        issues.add(getContext().createConfigIssue(
                Groups.HIVE.name(),
                "hiveConfDir",
                Errors.HIVE_06,
                coreSite.getName(),
                this.hiveConfDir)
        );
      } else {
        hiveConf.addResource(new Path(coreSite.getAbsolutePath()));
      }

      if (!hdfsSite.exists()) {
        issues.add(getContext().createConfigIssue(
                Groups.HIVE.name(),
                "hiveConfDir",
                Errors.HIVE_06,
                hdfsSite.getName(),
                this.hiveConfDir)
        );
      } else {
        hiveConf.addResource(new Path(hdfsSite.getAbsolutePath()));
      }

      if (!hiveSite.exists()) {
        issues.add(getContext().createConfigIssue(
                Groups.HIVE.name(),
                "hiveConfDir",
                Errors.HIVE_06,
                hiveSite.getName(),
                this.hiveConfDir)
        );
      } else {
        hiveConf.addResource(new Path(hiveSite.getAbsolutePath()));
      }
    } else {
      issues.add(getContext().createConfigIssue(Groups.HIVE.name(), "hiveConfDir", Errors.HIVE_07, this.hiveConfDir));
    }
  }

  private boolean validateHiveThriftUrl(List<ConfigIssue> issues) {
    // by this point in execution, the metastore ui was either provided in conf or overridden from UI.
    String[] uriStrings = hiveConf.get(HIVE_METASTORE_URI).split(",");
    for (String uriString : uriStrings) {
      try {
        URI uri = new URI(uriString);
        if (uri.getHost() == null || uri.getPort() == -1) {
          issues.add(getContext().createConfigIssue(Groups.HIVE.name(), "hiveUrl", Errors.HIVE_14, uriString));
          return false;
        }
      } catch (URISyntaxException e) {
        issues.add(getContext().createConfigIssue(Groups.HIVE.name(), "hiveUrl", Errors.HIVE_14, uriString));
        return false;
      }
    }
    return true;
  }

  private boolean validColumnMappings(List<ConfigIssue> issues) {
    boolean isValid = true;
    Set<String> columns = new HashSet<>(issues.size());
    for (FieldMappingConfig mapping : columnMappings) {
      if (!columns.add(mapping.columnName)) {
        isValid = false;
        issues.add(
            getContext().createConfigIssue(Groups.HIVE.name(), "columnMappings", Errors.HIVE_00, mapping.columnName)
        );
      }
    }
    return isValid;
  }

  @Override
  public void destroy() {
    if (hiveConnectionPool != null) {
      for (Map.Entry<HiveEndPoint, StreamingConnection> entry : hiveConnectionPool.asMap().entrySet()) {
        entry.getValue().close();
      }
    }
    super.destroy();
  }

  private TransactionBatch getBatch(int batchSize, HiveEndPoint endPoint) throws InterruptedException,
      StreamingException, ExecutionException {
    return hiveConnectionPool.get(endPoint).fetchTransactionBatch(batchSize, recordWriterPool.get(endPoint));
  }

  private DataGeneratorFactory createDataGeneratorFactory() {
    DataGeneratorFactoryBuilder builder = new DataGeneratorFactoryBuilder(
        getContext(),
        DataFormat.JSON.getGeneratorFormat()
    );
    return builder
        .setCharset(StandardCharsets.UTF_8) // Only UTF-8 is supported.
        .setMode(Mode.MULTIPLE_OBJECTS)
        .build();
  }

  @Override
  public void write(Batch batch) throws StageException {
    Map<HiveEndPoint, TransactionBatch> transactionBatches = new HashMap<>();
    Iterator<Record> it = batch.getRecords();

    if (!it.hasNext()) {
      // No records - take the opportunity to clean up the cache so that we don't hold on to memory indefinitely
      hiveConnectionCacheCleaner.periodicCleanUp();
      recordWriterCacheCleaner.periodicCleanUp();
    }

    while (it.hasNext()) {
      Record record = it.next();

      // Check that record has all required fields (partition columns).
      List<String> missingPartitions = getMissingRequiredFields(record, partitionsToFields);

      if (missingPartitions.size() == 0) {
        try {
          HiveEndPoint endPoint = getEndPointForRecord(record);

          TransactionBatch hiveBatch = transactionBatches.get(endPoint);
          if (null == hiveBatch || 0 == hiveBatch.remainingTransactions()) {
            hiveBatch = getBatch(txnBatchSize, endPoint);
            transactionBatches.put(endPoint, hiveBatch);
          }

          hiveBatch.beginNextTransaction();

          ByteArrayOutputStream bytes = new ByteArrayOutputStream(bufferLimit);
          DataGenerator generator = dataGeneratorFactory.getGenerator(bytes);

          // Transform record for field mapping overrides
          applyCustomMappings(record);

          // Remove Partition fields
          for (String fieldPath : partitionsToFields.values()) {
            record.delete(fieldPath);
          }
          generator.write(record);
          generator.close();

          hiveBatch.write(bytes.toByteArray());
          hiveBatch.commit();
        } catch (InterruptedException | StreamingException | IOException e) {
          LOG.error("Error processing batch: {}", e.toString(), e);
          throw new StageException(Errors.HIVE_01, e.toString(), e);
        } catch (ExecutionException e) {
          LOG.error("Error processing batch: {}", e.getCause().toString(), e);
          throw new StageException(Errors.HIVE_01, e.getCause().toString(), e);
        } catch (OnRecordErrorException e) {
          errorRecordHandler.onError(
              new OnRecordErrorException(
                  record,
                  e.getErrorCode(),
                  e.getParams()
              )
          );
        }
      } else {
        if (missingPartitions.size() != 0) {
          errorRecordHandler.onError(
              new OnRecordErrorException(
                  record,
                  Errors.HIVE_08,
                  StringUtils.join(",", missingPartitions)
              )
          );
        }
      }
    }

    for (TransactionBatch transactionBatch : transactionBatches.values()) {
      try {
        transactionBatch.close();
      } catch (InterruptedException | StreamingException e) {
        LOG.error("Failed to close transaction batch: {}", e.toString(), e);
      }
    }
  }

  private void applyCustomMappings(Record record) {
    for (Map.Entry<String, String> entry : columnsToFields.entrySet()) {
      if (!entry.getValue().equals(SDC_FIELD_SEP + entry.getKey())) {
        // This is a custom mapping
        if (record.has(entry.getValue())) {
          // The record has the requested field, rename it to match the column name.
          record.set(SDC_FIELD_SEP + entry.getKey(), record.get(entry.getValue()));
          // Remove the original field
          record.delete(entry.getValue());
        }
      }
    }
  }

  private List<String> getMissingRequiredFields(Record record, Map<String, String> mappings) {
    List<String> missingFields = new ArrayList<>(mappings.size());
    for (Map.Entry<String, String> mapping : mappings.entrySet()) {
      if (!record.has(mapping.getValue())) {
        missingFields.add(mapping.getValue());
      }
    }
    return missingFields;
  }

  private HiveEndPoint getEndPointForRecord(final Record record) throws OnRecordErrorException {
    List<String> partitions = new ArrayList<>(partitionsToFields.size());
    for (String partitionField : partitionsToFields.values()) {
      if (record.has(partitionField)) {
        partitions.add(record.get(partitionField).getValueAsString());
      }
    }
    HiveEndPoint endPoint;
    try {
      endPoint = new HiveEndPoint(hiveThriftUrl, schema, tableName, partitions);
    } catch (IllegalArgumentException e) {
      throw new OnRecordErrorException(Errors.HIVE_12, e.toString(), e);
    }
    return endPoint;
  }
}
