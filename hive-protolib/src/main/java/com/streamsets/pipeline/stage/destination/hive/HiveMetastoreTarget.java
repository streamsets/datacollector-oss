/**
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
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
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

public class HiveMetastoreTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(HiveMetastoreTarget.class.getCanonicalName());
  private static final String CONF = "conf";
  private static final String STORED_AS_AVRO = "storedAsAvro";
  private static final String EXTERNAL = "External";
  private static final Joiner JOINER = Joiner.on(".");

  private final HMSTargetConfigBean conf;

  private HiveQueryExecutor queryExecutor;
  private ErrorRecordHandler defaultErrorRecordHandler;
  private HMSCache hmsCache;

  public HiveMetastoreTarget(HMSTargetConfigBean conf) {
    this.conf = conf;
  }

  @Override
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
            .build(queryExecutor);
      } catch (StageException e) {
        issues.add(getContext().createConfigIssue(
            Groups.HIVE.name(),
            JOINER.join(CONF, "hiveConfigBean.hiveJDBCUrl"),
            Errors.HIVE_01,
            e.getMessage()
        ));
      }
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
        String location = HiveMetastoreUtil.getLocation(metadataRecord);

        TBLPropertiesInfoCacheSupport.TBLPropertiesInfo tblPropertiesInfo = HiveMetastoreUtil.getCacheInfo(
            hmsCache,
            HMSCacheType.TBLPROPERTIES_INFO,
            qualifiedTableName
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
              hmpDataFormat
          );
        } else {
          handlePartitionAddition(metadataRecord, qualifiedTableName, location, queryExecutor);
        }
      } catch (HiveStageCheckedException e) {
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
      String location,
      String databaseName,
      String tableName,
      HiveQueryExecutor hiveQueryExecutor,
      TBLPropertiesInfoCacheSupport.TBLPropertiesInfo tblPropertiesInfo,
      HMPDataFormat dataFormat
  ) throws StageException {
    //Schema Change
    String qualifiedTableName = HiveMetastoreUtil.getQualifiedTableName(databaseName, tableName);
    HMSCacheType cacheType = HMSCacheType.TYPE_INFO;
    TypeInfoCacheSupport.TypeInfo cachedColumnTypeInfo = HiveMetastoreUtil.getCacheInfo(
        hmsCache,
        cacheType,
        qualifiedTableName
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
          qualifiedTableName
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
      HiveMetastoreEvents.NEW_TABLE.create(getContext())
        .with("table", qualifiedTableName)
        .withStringMap("columns", Collections.<String, Object>unmodifiableMap(newColumnTypeInfo))
        .withStringMap("partitions", Collections.<String, Object>unmodifiableMap(partitionTypeInfo))
        .createAndSend();

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
            qualifiedTableName
          );
        }

        //Add Columns
        hiveQueryExecutor.executeAlterTableAddColumnsQuery(qualifiedTableName, columnDiff);

        if (!conf.storedAsAvro) {
          hiveQueryExecutor.executeAlterTableSetTblPropertiesQuery(qualifiedTableName, schemaPath);
        }
        cachedColumnTypeInfo.updateState(columnDiff);

        HiveMetastoreEvents.NEW_COLUMNS.create(getContext())
          .with("table", qualifiedTableName)
          .withStringMap("columns", Collections.<String, Object>unmodifiableMap(columnDiff))
          .createAndSend();
      }
    }
  }

  private String storeSchemaOnHDFS(
    Record metadataRecord,
    String avroSchema,
    String location,
    String databaseName,
    String tableName,
    String qualifiedTableName
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
    HiveMetastoreEvents.AVRO_SCHEMA_STORED.create(getContext())
      .with("table", qualifiedTableName)
      .with("avro_schema", avroSchema)
      .with("schema_location", schemaPath)
      .createAndSend();
    return schemaPath;
  }

  private void handlePartitionAddition(
      Record metadataRecord,
      String qualifiedTableName,
      String location,
      HiveQueryExecutor hiveQueryExecutor
  ) throws StageException {
    //Partition Addition
    TypeInfoCacheSupport.TypeInfo cachedTypeInfo = hmsCache.getOrLoad(
        HMSCacheType.TYPE_INFO,
        qualifiedTableName
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
        qualifiedTableName
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
          location
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

      HiveMetastoreEvents.NEW_PARTITION.create(getContext())
        .with("table", qualifiedTableName)
        .withStringMap("partition", Collections.<String, Object>unmodifiableMap(partitionValMap))
        .createAndSend();
    }
  }
}
