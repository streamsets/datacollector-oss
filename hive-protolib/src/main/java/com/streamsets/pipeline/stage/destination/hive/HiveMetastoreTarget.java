/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.hive;

import com.streamsets.pipeline.api.Batch;
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
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HiveMetastoreTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(HiveMetastoreTarget.class.getCanonicalName());
  private static final String CONF = "conf";
  private static final String STORED_AS_AVRO = "storedAsAvro";
  private static final String EXTERNAL = "External";

  private final HMSTargetConfigBean conf;

  private ErrorRecordHandler defaultErrorRecordHandler;
  private HMSCache hmsCache;

  public HiveMetastoreTarget(HMSTargetConfigBean conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    conf.init(getContext(), CONF, issues);
    if (issues.isEmpty()) {
      defaultErrorRecordHandler = new DefaultErrorRecordHandler(getContext());
      hmsCache =  HMSCache.newCacheBuilder()
          .addCacheTypeSupport(
              Arrays.asList(
                  HMSCacheType.TBLPROPERTIES_INFO,
                  HMSCacheType.TYPE_INFO,
                  HMSCacheType.PARTITION_VALUE_INFO
              )
          )
          .maxCacheSize(conf.hiveConfigBean.maxCacheSize)
          .build();
    }
    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> recordIterator = batch.getRecords();
    while (recordIterator.hasNext()) {
      Record metadataRecord = recordIterator.next();
      try {
        HiveMetastoreUtil.validateMetadataRecordForRecordTypeAndVersion(metadataRecord);
        String tableName = HiveMetastoreUtil.getTableName(metadataRecord);
        String databaseName = HiveMetastoreUtil.getDatabaseName(metadataRecord);
        String qualifiedTableName = HiveMetastoreUtil.getQualifiedTableName(databaseName, tableName);
        String resolvedJDBCUrl = HiveMetastoreUtil.resolveJDBCUrl(
            conf.hiveConfigBean.getElEval(),
            conf.hiveConfigBean.hiveJDBCUrl,
            metadataRecord
        );
        String location = HiveMetastoreUtil.getLocation(metadataRecord);
        HiveQueryExecutor hiveQueryExecutor = new HiveQueryExecutor(resolvedJDBCUrl, conf.hiveConfigBean.getUgi());
        TBLPropertiesInfoCacheSupport.TBLPropertiesInfo tblPropertiesInfo = hmsCache.getOrLoad(
            HMSCacheType.TBLPROPERTIES_INFO,
            resolvedJDBCUrl,
            qualifiedTableName,
            conf.hiveConfigBean.getUgi()
        );

        if (tblPropertiesInfo != null && tblPropertiesInfo.isStoredAsAvro() != conf.storedAsAvro) {
          LOG.warn(
              Utils.format(
                  Errors.HIVE_23.getMessage(),
                  STORED_AS_AVRO,
                  conf.storedAsAvro,
                  tblPropertiesInfo.isStoredAsAvro()
              )
          );
        }

        if (HiveMetastoreUtil.isSchemaChangeRecord(metadataRecord)) {
          handleSchemaChange(metadataRecord, qualifiedTableName, resolvedJDBCUrl, location, hiveQueryExecutor, tblPropertiesInfo);
        } else {
          handlePartitionAddition(metadataRecord, qualifiedTableName, resolvedJDBCUrl, location, hiveQueryExecutor);
        }
      } catch (StageException e) {
        LOG.error("Error processing record: {}", e);
        defaultErrorRecordHandler.onError(new OnRecordErrorException(metadataRecord, e.getErrorCode(), e.getParams()));
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
      String qualifiedTableName,
      String resolvedJDBCUrl,
      String location,
      HiveQueryExecutor hiveQueryExecutor,
      TBLPropertiesInfoCacheSupport.TBLPropertiesInfo tblPropertiesInfo
  ) throws StageException {
    //Schema Change
    HMSCacheType cacheType = HMSCacheType.TYPE_INFO;
    TypeInfoCacheSupport.TypeInfo cachedColumnTypeInfo = hmsCache.getOrLoad(
        cacheType,
        resolvedJDBCUrl,
        qualifiedTableName,
        conf.hiveConfigBean.getUgi()
    );
    LinkedHashMap<String, HiveTypeInfo> newColumnTypeInfo = HiveMetastoreUtil.getColumnNameType(metadataRecord);
    LinkedHashMap<String, HiveTypeInfo> partitionTypeInfo = HiveMetastoreUtil.getPartitionNameType(metadataRecord);
    boolean isInternal = HiveMetastoreUtil.getInternalField(metadataRecord);
    String schemaPath = null;

    if (tblPropertiesInfo != null && tblPropertiesInfo.isExternal() == isInternal) {
      throw new StageException(Errors.HIVE_23, EXTERNAL, !isInternal, tblPropertiesInfo.isExternal());
    }

    if (cachedColumnTypeInfo == null) {
      //Table Does not exist use the schema from the metadata record as is.
      if (!conf.storedAsAvro) {
        schemaPath = HiveMetastoreUtil.serializeSchemaToHDFS(
            conf.getHDFSUgi(),
            conf.getFileSystem(),
            location,
            HiveMetastoreUtil.getAvroSchema(metadataRecord)
        );
      }
      //Create Table
      hiveQueryExecutor.executeCreateTableQuery(
          qualifiedTableName,
          newColumnTypeInfo,
          partitionTypeInfo,
          conf.storedAsAvro,
          schemaPath,
          isInternal
      );

      hmsCache.put(
          cacheType,
          qualifiedTableName,
          new TypeInfoCacheSupport.TypeInfo(newColumnTypeInfo, partitionTypeInfo)
      );
    } else {
      //Diff to get new columns.
      LinkedHashMap<String, HiveTypeInfo> columnDiff = cachedColumnTypeInfo.getDiff(newColumnTypeInfo);
      if (!columnDiff.isEmpty()) {
        //Regenerate schema with all the columns. (This will factor for in existing, new and missing columns).
        if (!conf.storedAsAvro) {
          Map<String, HiveTypeInfo> mergedTypeInfo = new LinkedHashMap<>(cachedColumnTypeInfo.getColumnTypeInfo());
          mergedTypeInfo.putAll(columnDiff);
          schemaPath = HiveMetastoreUtil.serializeSchemaToHDFS(
              conf.getHDFSUgi(),
              conf.getFileSystem(),
              location,
              HiveMetastoreUtil.generateAvroSchema(mergedTypeInfo, qualifiedTableName)
          );
        }

        //Add Columns
        hiveQueryExecutor.executeAlterTableAddColumnsQuery(qualifiedTableName, columnDiff);

        if (!conf.storedAsAvro) {
          hiveQueryExecutor.executeAlterTableSetTblPropertiesQuery(qualifiedTableName, schemaPath);
        }
        cachedColumnTypeInfo.updateState(columnDiff);
      }
    }
  }

  private void handlePartitionAddition(
      Record metadataRecord,
      String qualifiedTableName,
      String resolvedJDBCUrl,
      String location,
      HiveQueryExecutor hiveQueryExecutor
  ) throws StageException {
    //Partition Addition
    TypeInfoCacheSupport.TypeInfo cachedTypeInfo = hmsCache.getOrLoad(
        HMSCacheType.TYPE_INFO,
        resolvedJDBCUrl,
        qualifiedTableName,
        conf.hiveConfigBean.getUgi()
    );

    if (cachedTypeInfo == null) {
      throw new StageException(Errors.HIVE_25, qualifiedTableName);
    }

    HMSCacheType hmsCacheType = HMSCacheType.PARTITION_VALUE_INFO;
    PartitionInfoCacheSupport.PartitionInfo cachedPartitionInfo = hmsCache.getOrLoad(
        hmsCacheType,
        resolvedJDBCUrl,
        qualifiedTableName,
        conf.hiveConfigBean.getUgi()
    );
    LinkedHashMap<String, String> partitionValMap = HiveMetastoreUtil.getPartitionNameValue(metadataRecord);
    Set<LinkedHashMap <String, String>> partitionInfoDiff =
        new LinkedHashSet<>(Collections.singletonList(partitionValMap));
    partitionInfoDiff = (cachedPartitionInfo != null)?
        cachedPartitionInfo.getDiff(partitionInfoDiff) : partitionInfoDiff;
    if (!partitionInfoDiff.isEmpty()) {
      hiveQueryExecutor.executeAlterTableAddPartitionQuery(
          qualifiedTableName,
          partitionValMap,
          cachedTypeInfo.getPartitionTypeInfo(),
          location
      );
      if (cachedPartitionInfo != null) {
        cachedPartitionInfo.updateState(partitionInfoDiff);
      } else {
        hmsCache.put(hmsCacheType, qualifiedTableName, new PartitionInfoCacheSupport.PartitionInfo(partitionInfoDiff));
      }
    }
  }
}
