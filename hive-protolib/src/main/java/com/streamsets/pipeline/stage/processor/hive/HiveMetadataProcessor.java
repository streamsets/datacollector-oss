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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.hive;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.RecordProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;
import com.streamsets.pipeline.stage.lib.hive.HiveType;
import com.streamsets.pipeline.stage.lib.hive.AvroSchemaInfoCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.TypeInfoCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.PartitionInfoCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.HMSCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.HMSCache;
import com.streamsets.pipeline.stage.lib.hive.HMSCacheType;
import com.streamsets.pipeline.stage.destination.hive.HiveConfigBean;

import java.util.List;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.Collections;
import java.util.Arrays;

public class HiveMetadataProcessor extends RecordProcessor {

  // mandatory configuration values
  private static final String HIVE_DB_NAME = "dbNameEL";
  private static final String HIVE_TABLE_NAME = "tableNameEL";
  private static final String HIVE_JDBC_URL = "hiveJDBCUrl";
  private final String databaseEL;
  private final String tableEL;
  private final String hiveJDBCUrl;
  private ELEval ELval;
  private final boolean externalTable;

  // Triplet of partition name, type and value expression obtained from configration
  private List<PartitionConfig> partitionConfigList;
  // List of partition name and value type.
  private LinkedHashMap<String, HiveType> partitionTypeInfo;

  // Optional configuration values
  private String tablePathTemplate = null;
  private String partitionPathTemplate = null;
  private String warehouseDirectory = null;

  private HMSCache cache;

  private String HDFSLane;
  private String HMSLane;
  private int maxCacheSize;
  private ErrorRecordHandler toError;

  public HiveMetadataProcessor(
      String databaseEL,
      String tableEL,
      List<PartitionConfig> partition_list,
      boolean externalTable,
      String warehouseDirectory,
      String tablePathTemplate,
      String partitionPathTemplate,
      HiveConfigBean hiveConfig) {
    this.databaseEL = databaseEL;
    this.tableEL = tableEL;
    this.externalTable = externalTable;
    this.partitionConfigList = partition_list;
    this.maxCacheSize = hiveConfig.maxCacheSize;
    this.hiveJDBCUrl = hiveConfig.hiveJDBCUrl;
    if (externalTable){
      this.tablePathTemplate = tablePathTemplate;
      this.partitionPathTemplate = partitionPathTemplate;
    } else {
      this.warehouseDirectory = warehouseDirectory;
    }
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues =  super.init();

    ELval = getContext().createELEval(HIVE_DB_NAME); //TODO

    // TODO add validation and hive-site.xml

    if (partitionConfigList.isEmpty()) {
      issues.add(getContext().createConfigIssue(
          Groups.HIVE.name(),
          "partition_list",
          Errors.HiveMetadata_01,
          "Partition Configuration"));
    }
    if (!externalTable) {
      //TODO validation on warehouseDirectory
    }

    for (PartitionConfig partition: partitionConfigList){
      // Expression for partition value is not automatically checked on Preview
      if (partition.valueEL.isEmpty()){
        issues.add(getContext().createConfigIssue(
            Groups.HIVE.name(),
            "partition_list",
            Errors.HiveMetadata_02,
            "Partition Configuration"));
      }
    }

    if (issues.isEmpty()) {
      toError = new DefaultErrorRecordHandler(getContext());
      partitionTypeInfo = new LinkedHashMap<>();
      for (PartitionConfig partition: partitionConfigList) {
        partitionTypeInfo.put(partition.name, partition.valueType);
      }
      HMSLane = getContext().getOutputLanes().get(0);
      HDFSLane = getContext().getOutputLanes().get(1);
      // load cache
      cache = HMSCache.newCacheBuilder()
          .addCacheTypeSupport(
              Arrays.asList(
                  HMSCacheType.TYPE_INFO,
                  HMSCacheType.PARTITION_VALUE_INFO,
                  HMSCacheType.AVRO_SCHEMA_INFO
              )
          )
          .maxCacheSize(maxCacheSize)
          .build();
    }
    return issues;
  }

  @Override
  protected void process(Record record, BatchMaker batchMaker) throws StageException {
    ELVars variables = getContext().createELVars();
    RecordEL.setRecordInContext(variables, record);
    String dbName = resolveEL(variables, databaseEL);
    String tableName = resolveEL(variables,tableEL);
    String warehouseDir = resolveEL(variables, warehouseDirectory);
    LinkedHashMap<String, HiveType> recordStructure = null;
    LinkedHashMap<String, String> partitionValMap = null;
    String avroSchema = "";
    boolean needToRoll = false;

    // First, find out if this record has all necessary data to process
    try {
      if (dbName.isEmpty() || tableName.isEmpty()) {
        throw new StageException(Errors.HiveMetadata_03, dbName.isEmpty() ? databaseEL : tableEL);
      }
      if (warehouseDir.isEmpty()) {
        throw new StageException(Errors.HiveMetadata_03, warehouseDirectory);
      }
      String qualifiedName = HiveMetastoreUtil.getQualifiedTableName(dbName, tableName);

      // Obtain the record structure from current record
      recordStructure = HiveMetastoreUtil.convertRecordToHMSType(record);
      // Obtain all te partition values from record
      partitionValMap = getPartitionValuesFromRecord(variables);

      TypeInfoCacheSupport.TypeInfo tableCache
          = (TypeInfoCacheSupport.TypeInfo) getCacheInfo(HMSCacheType.TYPE_INFO, qualifiedName);

      AvroSchemaInfoCacheSupport.AvroSchemaInfo schemaCache
          = (AvroSchemaInfoCacheSupport.AvroSchemaInfo) getCacheInfo(
          HMSCacheType.AVRO_SCHEMA_INFO,
          qualifiedName
      );

      // send Schema Change metadata when tableCache is null or schema is changed
      if (tableCache == null || detectSchemaChange(recordStructure,tableCache)) {
        needToRoll = true;
        handleSchemaChange(dbName, tableName, recordStructure, warehouseDir, avroSchema, batchMaker, qualifiedName,
            tableCache, schemaCache);
      }
      // Send new partition metadata if new partition is detected.
      PartitionInfoCacheSupport.PartitionInfo pCache
          = (PartitionInfoCacheSupport.PartitionInfo) getCacheInfo(HMSCacheType.PARTITION_VALUE_INFO, qualifiedName);
      Set<LinkedHashMap<String, String>> diff = detectNewPartition(partitionValMap, pCache);
      if (diff != null) {
        handleNewPartition(partitionValMap, pCache, dbName, tableName, warehouseDir, batchMaker, qualifiedName, diff);
      }
      // TODO: Fill in header to send to HDFS
      // roll = needToRoll
      // targetDirectory: resolve directory path
      // avroSchema = avroSchema
    } catch (StageException e) {
        toError.onError(new OnRecordErrorException(record, Errors.HiveMetadata_03, e.getMessage()));
    }

  }

  // Get cached data from cache. First call getIfPresent to obtain data from local cache.
  // If not exist, load from HMS
  private HMSCacheSupport.HMSCacheInfo getCacheInfo(HMSCacheType cacheType, String qualifiedName)
      throws StageException {
    HMSCacheSupport.HMSCacheInfo cacheInfo = null;

    cacheInfo = cache.getIfPresent(  // Or better to keep this in this class?
        cacheType,
        qualifiedName);

    if (cacheType != HMSCacheType.AVRO_SCHEMA_INFO && cacheInfo == null) {
      // Try loading by executing HMS query
      cacheInfo = cache.getOrLoad(
          cacheType,
          hiveJDBCUrl,
          qualifiedName
      );
    }
    return cacheInfo;
  }

  // ------------ Handle New Schema ------------------------//

  boolean detectSchemaChange(
      LinkedHashMap<String, HiveType> recordStructure,
      HMSCacheSupport.HMSCacheInfo cache) throws StageException
  {
    LinkedHashMap<String, HiveType> columnDiff = null;
    // compare the record structure vs cache
    if (cache != null) {
      columnDiff = ((TypeInfoCacheSupport.TypeInfo)cache).getDiff(recordStructure);
    }
    return columnDiff.isEmpty()? false : true;
  }

  private Record generateSchemaChangeRecord(
      String database,
      String tableName,
      LinkedHashMap<String, HiveType> columnList,
      LinkedHashMap<String, HiveType> partitionTypeList,
      String location,
      String avroSchema) throws StageException
  {
    Record metadataRecord = getContext().createRecord("");
    Field matadata =HiveMetastoreUtil.newSchemaMetadataFieldBuilder(
        database,
        tableName,
        columnList,
        partitionTypeList,
        !externalTable,  // need to flip boolean to send if this is internal table
        location,
        avroSchema
    );
    metadataRecord.set(matadata);
    return metadataRecord;
  }

  private void handleSchemaChange(
      String dbName,
      String tableName,
      LinkedHashMap<String, HiveType> recordStructure,
      String warehouseDir, String avroSchema,
      BatchMaker batchMaker,
      String qualifiedName,
      TypeInfoCacheSupport.TypeInfo tableCache,
      AvroSchemaInfoCacheSupport.AvroSchemaInfo schemaCache)
      throws StageException {
    // TODO: Need to generate avro schema from here based on recordStructure

    Record r = generateSchemaChangeRecord(dbName, tableName, recordStructure, partitionTypeInfo, warehouseDir, avroSchema);
    batchMaker.addRecord(r, HMSLane);
    // update or insert the new record structure to cache
    if (tableCache != null) {
      tableCache.updateState(recordStructure);
      schemaCache.updateState(avroSchema);
    } else {
      cache.put(
          HMSCacheType.TYPE_INFO,
          qualifiedName,
          new TypeInfoCacheSupport.TypeInfo(recordStructure, partitionTypeInfo)
      );
      cache.put(
          HMSCacheType.AVRO_SCHEMA_INFO,
          qualifiedName,
          new AvroSchemaInfoCacheSupport.AvroSchemaInfo(avroSchema)
      );
    }
  }

  // -------- Handle New Partitions ------------------//

  private Set<LinkedHashMap<String, String>> detectNewPartition(
      LinkedHashMap<String, String> partitionValMap,
      PartitionInfoCacheSupport.PartitionInfo pCache) throws StageException{
    // Start evaluating partition value
    Set<LinkedHashMap <String, String>> partitionInfoDiff
        = new LinkedHashSet<>(Collections.singletonList(partitionValMap));
    partitionInfoDiff
        = (pCache != null)? pCache.getDiff(partitionInfoDiff) : partitionInfoDiff;
    if (pCache == null || !partitionInfoDiff.isEmpty()){
      return partitionInfoDiff;
    } else {
      return null;
    }
  }

  private LinkedHashMap<String, String> getPartitionValuesFromRecord(ELVars variables)
      throws StageException
  {
    LinkedHashMap<String, String> values = new LinkedHashMap<>();
    for (PartitionConfig pName: partitionConfigList){
      String ret = resolveEL(variables, pName.valueEL);
      if (ret == null || ret.isEmpty()){
        // If no partition value is found in record, this record goes to Error Record
        throw new StageException(Errors.HiveMetadata_03, pName.valueEL );
      } else {
        values.put(pName.name, ret);
      }
      values.put(pName.name, ret);
    }
    return values;
  }

  private Record generateNewPartitionRecord(
      String database,
      String tableName,
      LinkedHashMap<String, String> partition_list,
      String location) throws StageException {
    Record metadataRecord = getContext().createRecord("");
    Field metadata = HiveMetastoreUtil.newPartitionMetadataFieldBuilder(
        database,
        tableName,
        partition_list,
        location
    );
    metadataRecord.set(metadata);
    return metadataRecord;
  }

  private void handleNewPartition(
      LinkedHashMap<String, String> partitionValMap,
      PartitionInfoCacheSupport.PartitionInfo pCache,
      String database,
      String tableName,
      String location,
      BatchMaker batchMaker,
      String qualifiedName,
      Set<LinkedHashMap<String, String>> diff
  ) throws StageException{

    Record r = generateNewPartitionRecord(database, tableName, partitionValMap, location);
    batchMaker.addRecord(r, HMSLane);
    if (pCache != null) {
      pCache.updateState(diff);
    } else {
      cache.put(
          HMSCacheType.PARTITION_VALUE_INFO,
          qualifiedName,
          new PartitionInfoCacheSupport.PartitionInfo(diff)
      );
    }
  }

  // Resolve expression from record
  protected String resolveEL(ELVars variables, String val) throws ELEvalException
  {
    return ELval.eval(variables, val, String.class);
  }
}
