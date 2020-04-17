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

import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.lib.hive.Errors;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;
import com.streamsets.pipeline.stage.lib.hive.HiveQueryExecutor;
import com.streamsets.pipeline.stage.lib.hive.cache.HMSCache;
import com.streamsets.pipeline.stage.lib.hive.cache.HMSCacheType;
import com.streamsets.pipeline.stage.lib.hive.cache.PartitionInfoCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.cache.TBLPropertiesInfoCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.cache.TypeInfoCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.exceptions.HiveStageCheckedException;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveTypeInfo;
import com.streamsets.pipeline.stage.processor.hive.HMPDataFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class HiveMetastoreTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(HiveMetastoreTarget.class.getCanonicalName());
  private static final String CONF = "conf";
  private static final String STORED_AS_AVRO = "storedAsAvro";
  private static final String EXTERNAL = "External";
  private static final Joiner JOINER = Joiner.on(".");
  private static final String KEY_LOCKS = "table-locks";

  private final HMSTargetConfigBean conf;

  private HiveQueryExecutor queryExecutor;
  private ErrorRecordHandler defaultErrorRecordHandler;
  private HMSCache hmsCache;
  private LoadingCache<String, Lock> tableLocks;

  public HiveMetastoreTarget(HMSTargetConfigBean conf) {
    this.conf = conf;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    defaultErrorRecordHandler = new DefaultErrorRecordHandler(getContext());
    conf.init(getContext(), CONF, issues);
    if (issues.isEmpty()) {
      try {
        // We have exactly one instance of the query executor per stage to calculate it's metrics
        queryExecutor = new HiveQueryExecutor(conf.hiveConfigBean, getContext());

        hmsCache = HMSCache.newCacheBuilder()
            .addCacheTypeSupport(
                Arrays.asList(
                    HMSCacheType.TBLPROPERTIES_INFO,
                    HMSCacheType.TYPE_INFO,
                    HMSCacheType.PARTITION_VALUE_INFO
                )
            )
            .maxCacheSize(conf.hiveConfigBean.maxCacheSize)
            .build();
      } catch (StageException e) {
        issues.add(getContext().createConfigIssue(
            Groups.HIVE.name(),
            JOINER.join(CONF, "hiveConfigBean.hiveJDBCUrl"),
            Errors.HIVE_01,
            e.getMessage()
        ));
      }

      // Table locks is a cache shared with all runners that contains locks for all tables. Locks inside will expire
      // in 15 minutes -- idea is that if a thread holds a lock for more then 15 minutes, then we have bigger issues to
      // worry about than that two threads could be altering two tables.
      Map<String, Object> runnerSharedMap = getContext().getStageRunnerSharedMap();
      synchronized (runnerSharedMap) {
        tableLocks = (LoadingCache<String, Lock>) runnerSharedMap.computeIfAbsent(KEY_LOCKS,
          key -> CacheBuilder.newBuilder()
            .expireAfterAccess(15, TimeUnit.MINUTES)
            .build(new CacheLoader<String, Lock>() {
              @Override
              public Lock load(String key) throws Exception {
                return new ReentrantLock();
              }
            })
        );
      }
    }
    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> recordIterator = batch.getRecords();
    while (recordIterator.hasNext()) {
      Record metadataRecord = recordIterator.next();
      Lock tableLock = null;
      try {
        HiveMetastoreUtil.validateMetadataRecordForRecordTypeAndVersion(metadataRecord);
        String tableName = HiveMetastoreUtil.getTableName(metadataRecord);
        String databaseName = HiveMetastoreUtil.getDatabaseName(metadataRecord);
        String qualifiedTableName = HiveMetastoreUtil.getQualifiedTableName(databaseName, tableName);
        String location = HiveMetastoreUtil.getLocation(metadataRecord);

        // Get resolved headers (for output Event)
        Map<String, String> resolvedHeaders = new LinkedHashMap<>();
        if(!conf.isHeadersEmpty()) {
          resolvedHeaders = conf.getResolvedHeaders(getContext(), metadataRecord);
        }

        // Get exclusive lock for this table
        tableLock = tableLocks.get(qualifiedTableName);
        tableLock.lock();

        TBLPropertiesInfoCacheSupport.TBLPropertiesInfo tblPropertiesInfo = HiveMetastoreUtil.getCacheInfo(
            hmsCache,
            HMSCacheType.TBLPROPERTIES_INFO,
            qualifiedTableName,
            queryExecutor
        );

        // get dataFormat from metadataRecord
        String dataFormat = HiveMetastoreUtil.getDataFormat(metadataRecord);
        HMPDataFormat hmpDataFormat = null;

        try {
          hmpDataFormat = HMPDataFormat.valueOf(dataFormat);
        } catch (Exception ex) {
          throw new HiveStageCheckedException(
              Errors.HIVE_37,
              dataFormat
          );
        }

        if (tblPropertiesInfo != null) {
          HiveMetastoreUtil.validateTblPropertiesInfo(hmpDataFormat, tblPropertiesInfo, qualifiedTableName);

          if (hmpDataFormat == HMPDataFormat.AVRO && tblPropertiesInfo.isStoredAsAvro() != conf.storedAsAvro) {
            LOG.warn(
                Utils.format(
                    Errors.HIVE_23.getMessage(),
                    STORED_AS_AVRO,
                    conf.storedAsAvro,
                    tblPropertiesInfo.isStoredAsAvro()
                )
            );
          }
        }

        if (HiveMetastoreUtil.isSchemaChangeRecord(metadataRecord)) {
          handleSchemaChange(
              metadataRecord,
              location,
              databaseName,
              tableName,
              queryExecutor,
              tblPropertiesInfo,
              hmpDataFormat,
              resolvedHeaders
          );
        } else {
          boolean customLocation = HiveMetastoreUtil.getCustomLocation(metadataRecord);
          handlePartitionAddition(metadataRecord, qualifiedTableName, location, customLocation, queryExecutor,
              resolvedHeaders);
        }
      } catch (HiveStageCheckedException e) {
        LOG.error("Error processing record: {}", e);
        defaultErrorRecordHandler.onError(new OnRecordErrorException(metadataRecord, e.getErrorCode(), e.getParams()));
      } catch (ExecutionException e) {
        LOG.error("Can't retrieve lock for table: {}", e);
        defaultErrorRecordHandler.onError(new OnRecordErrorException(metadataRecord, Errors.HIVE_01, e.toString()));
      } finally {
        if(tableLock != null) {
          tableLock.unlock();
        }
      }
    }
  }

  @Override
  public void destroy() {
    conf.destroy();
    super.destroy();
  }

  private void handleSchemaChange(
      Record metadataRecord,
      String location,
      String databaseName,
      String tableName,
      HiveQueryExecutor hiveQueryExecutor,
      TBLPropertiesInfoCacheSupport.TBLPropertiesInfo tblPropertiesInfo,
      HMPDataFormat dataFormat,
      Map<String, String> headers
  ) throws StageException {
    //Schema Change
    String qualifiedTableName = HiveMetastoreUtil.getQualifiedTableName(databaseName, tableName);
    HMSCacheType cacheType = HMSCacheType.TYPE_INFO;
    TypeInfoCacheSupport.TypeInfo cachedColumnTypeInfo = HiveMetastoreUtil.getCacheInfo(
        hmsCache,
        cacheType,
        qualifiedTableName,
        hiveQueryExecutor
    );
    LinkedHashMap<String, HiveTypeInfo> newColumnTypeInfo = HiveMetastoreUtil.getColumnNameType(metadataRecord);
    LinkedHashMap<String, HiveTypeInfo> partitionTypeInfo = HiveMetastoreUtil.getPartitionNameType(metadataRecord);
    boolean isInternal = HiveMetastoreUtil.getInternalField(metadataRecord);
    String schemaPath = null;

    if (tblPropertiesInfo != null && tblPropertiesInfo.isExternal() == isInternal) {
      throw new HiveStageCheckedException(Errors.HIVE_23, EXTERNAL, !isInternal, tblPropertiesInfo.isExternal());
    }

    if (cachedColumnTypeInfo == null) {
      //Table Does not exist use the schema from the metadata record as is.
      if (!conf.storedAsAvro) {
        schemaPath = storeSchemaOnHDFS(
          metadataRecord,
          HiveMetastoreUtil.getAvroSchema(metadataRecord),
          location,
          databaseName,
          tableName,
          qualifiedTableName,
          headers
        );
      }
      //Create Table
      hiveQueryExecutor.executeCreateTableQuery(
          qualifiedTableName,
          location,
          newColumnTypeInfo,
          partitionTypeInfo,
          conf.storedAsAvro,
          schemaPath,
          isInternal,
          dataFormat
      );

      hmsCache.put(
          cacheType,
          qualifiedTableName,
          new TypeInfoCacheSupport.TypeInfo(newColumnTypeInfo, partitionTypeInfo)
      );

      // Generate new table event
      EventRecord event = HiveMetastoreEvents.NEW_TABLE.create(getContext())
        .with("table", qualifiedTableName)
        .withStringMap("columns", Collections.<String, Object>unmodifiableMap(newColumnTypeInfo))
        .withStringMap("partitions", Collections.<String, Object>unmodifiableMap(partitionTypeInfo))
        .create();
      if (!conf.isHeadersEmpty()) {
        for ( Map.Entry<String, String> entry : headers.entrySet()) {
          event.getHeader().setAttribute(entry.getKey(), entry.getValue());
        }
      }
      getContext().toEvent(event);

    } else {
      //Diff to get new columns.
      LinkedHashMap<String, HiveTypeInfo> columnDiff = cachedColumnTypeInfo.getDiff(newColumnTypeInfo);
      if (!columnDiff.isEmpty()) {
        //Regenerate schema with all the columns. (This will factor for in existing, new and missing columns).
        if (!conf.storedAsAvro) {
          Map<String, HiveTypeInfo> mergedTypeInfo = new LinkedHashMap<>(cachedColumnTypeInfo.getColumnTypeInfo());
          mergedTypeInfo.putAll(columnDiff);
          schemaPath = storeSchemaOnHDFS(
            metadataRecord,
            HiveMetastoreUtil.generateAvroSchema(mergedTypeInfo, qualifiedTableName),
            location,
            databaseName,
            tableName,
            qualifiedTableName,
            headers
          );
          hiveQueryExecutor.executeAlterTableSetTblPropertiesQuery(qualifiedTableName, schemaPath);
        } else {
          //Add Columns
          hiveQueryExecutor.executeAlterTableAddColumnsQuery(qualifiedTableName, columnDiff);
        }
        cachedColumnTypeInfo.updateState(columnDiff);

        EventRecord event = HiveMetastoreEvents.NEW_COLUMNS.create(getContext())
          .with("table", qualifiedTableName)
          .withStringMap("columns", Collections.<String, Object>unmodifiableMap(columnDiff))
          .create();
        if (!conf.isHeadersEmpty()) {
          for ( Map.Entry<String, String> entry : headers.entrySet()) {
            event.getHeader().setAttribute(entry.getKey(), entry.getValue());
          }
        }
        getContext().toEvent(event);
      }
    }
  }

  private String storeSchemaOnHDFS(
    Record metadataRecord,
    String avroSchema,
    String location,
    String databaseName,
    String tableName,
    String qualifiedTableName,
    Map<String, String> headers
  ) throws StageException {
    String schemaPath = HiveMetastoreUtil.serializeSchemaToHDFS(
      conf.getHDFSUgi(),
      conf.getFileSystem(),
      location,
      conf.getSchemaFolderLocation(getContext(), metadataRecord),
      databaseName,
      tableName,
      avroSchema
    );
    EventRecord event = HiveMetastoreEvents.AVRO_SCHEMA_STORED.create(getContext())
      .with("table", qualifiedTableName)
      .with("avro_schema", avroSchema)
      .with("schema_location", schemaPath)
      .create();
    if (!conf.isHeadersEmpty()) {
      for ( Map.Entry<String, String> entry : headers.entrySet()) {
        event.getHeader().setAttribute(entry.getKey(), entry.getValue());
      }
    }
    getContext().toEvent(event);
    return schemaPath;
  }

  private void handlePartitionAddition(
      Record metadataRecord,
      String qualifiedTableName,
      String location,
      boolean customLocation,
      HiveQueryExecutor hiveQueryExecutor,
      Map<String, String> headers
  ) throws StageException {
    //Partition Addition
    TypeInfoCacheSupport.TypeInfo cachedTypeInfo = hmsCache.getOrLoad(
        HMSCacheType.TYPE_INFO,
        qualifiedTableName,
        hiveQueryExecutor
    );

    if (cachedTypeInfo == null) {
      throw new StageException(Errors.HIVE_25, qualifiedTableName);
    } else if (cachedTypeInfo.getPartitionTypeInfo().isEmpty()) {
      throw new HiveStageCheckedException(Errors.HIVE_27, qualifiedTableName);
    }

    HMSCacheType hmsCacheType = HMSCacheType.PARTITION_VALUE_INFO;
    PartitionInfoCacheSupport.PartitionInfo cachedPartitionInfo = HiveMetastoreUtil.getCacheInfo(
        hmsCache,
        hmsCacheType,
        qualifiedTableName,
        hiveQueryExecutor
    );
    LinkedHashMap<String, String> partitionValMap = HiveMetastoreUtil.getPartitionNameValue(metadataRecord);
    PartitionInfoCacheSupport.PartitionValues partitionValues =
        new PartitionInfoCacheSupport.PartitionValues(partitionValMap);

    HiveMetastoreUtil.validatePartitionInformation(cachedTypeInfo, partitionValMap, qualifiedTableName);

    Map<PartitionInfoCacheSupport.PartitionValues, String> partitionInfoDiff = new HashMap<>();
    partitionInfoDiff.put(partitionValues, location);

    partitionInfoDiff = (cachedPartitionInfo != null)? cachedPartitionInfo.getDiff(partitionInfoDiff) : partitionInfoDiff;
    if (!partitionInfoDiff.isEmpty()) {
      hiveQueryExecutor.executeAlterTableAddPartitionQuery(
          qualifiedTableName,
          partitionValMap,
          cachedTypeInfo.getPartitionTypeInfo(),
          customLocation ? location : null
      );
      if (cachedPartitionInfo != null) {
        cachedPartitionInfo.updateState(partitionInfoDiff);
      } else {
        hmsCache.put(
            hmsCacheType,
            qualifiedTableName,
            new PartitionInfoCacheSupport.PartitionInfo(partitionInfoDiff, queryExecutor, qualifiedTableName)
        );
      }

      EventRecord event = HiveMetastoreEvents.NEW_PARTITION.create(getContext())
        .with("table", qualifiedTableName)
        .withStringMap("partition", Collections.<String, Object>unmodifiableMap(partitionValMap))
        .create();
      if (!conf.isHeadersEmpty()) {
        for ( Map.Entry<String, String> entry : headers.entrySet()) {
          event.getHeader().setAttribute(entry.getKey(), entry.getValue());
        }
      }
      getContext().toEvent(event);
    }
  }
}
