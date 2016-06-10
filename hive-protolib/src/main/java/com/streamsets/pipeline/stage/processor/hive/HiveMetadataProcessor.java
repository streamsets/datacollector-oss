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

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.base.RecordProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.lib.hive.HiveConfigBean;
import com.streamsets.pipeline.stage.lib.hive.Groups;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;
import com.streamsets.pipeline.stage.lib.hive.cache.AvroSchemaInfoCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.cache.HMSCache;
import com.streamsets.pipeline.stage.lib.hive.cache.HMSCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.cache.HMSCacheType;
import com.streamsets.pipeline.stage.lib.hive.cache.PartitionInfoCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.cache.TBLPropertiesInfoCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.cache.TypeInfoCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.exceptions.HiveStageCheckedException;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveTypeInfo;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class HiveMetadataProcessor extends RecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(HiveMetadataProcessor.class.getCanonicalName());
  // mandatory configuration values
  private static final String HIVE_DB_NAME = "dbNameEL";
  private static final String HIVE_TABLE_NAME = "tableNameEL";
  private static final String HIVE_PARTITION_CONFIG_VALUE_EL = "valueEL";
  private static final String TABLE_PATH_TEMPLATE = "tablePathTemplate";
  private static final String PARTITION_PATH_TEMPLATE = "partitionPathTemplate";
  private static final String TIME_DRIVER = "timeDriver";

  protected static final String HDFS_HEADER_ROLL = "roll";
  protected static final String HDFS_HEADER_AVROSCHEMA = "avroSchema";
  protected static final String HDFS_HEADER_TARGET_DIRECTORY = "targetDirectory";
  public static final String DEFAULT_DB = "default";
  private final String databaseEL;
  private final String tableEL;
  private final boolean externalTable;
  private String internalWarehouseDir;
  private final String timeDriver;

  // Triplet of partition name, type and value expression obtained from configuration
  private List<PartitionConfig> partitionConfigList;
  // List of partition name and value type.
  private LinkedHashMap<String, HiveTypeInfo> partitionTypeInfo;

  // Optional configuration values
  private String tablePathTemplate;
  private String partitionPathTemplate; //TODO

  private HMSCache cache;

  private String hdfsLane;
  private String hmsLane;

  private HiveConfigBean hiveConfigBean;
  private ErrorRecordHandler errorRecordHandler;
  private HiveMetadataProcessorELEvals elEvals = new HiveMetadataProcessorELEvals();
  ;

  private static class HiveMetadataProcessorELEvals {
    private ELEval dbNameELEval;
    private ELEval tableNameELEval;
    private ELEval partitionValueELEval;
    private ELEval tablePathTemplateELEval;
    private ELEval timeDriverElEval;
    private ELEval partitionPathTemplateELEval;

    public void init(Stage.Context context) {
      dbNameELEval = context.createELEval(HIVE_DB_NAME);
      tableNameELEval = context.createELEval(HIVE_TABLE_NAME);
      partitionValueELEval = context.createELEval(HIVE_PARTITION_CONFIG_VALUE_EL);
      tablePathTemplateELEval = context.createELEval(TABLE_PATH_TEMPLATE);
      partitionPathTemplateELEval = context.createELEval(PARTITION_PATH_TEMPLATE);
      timeDriverElEval = context.createELEval(TIME_DRIVER);
    }
  }

  public HiveMetadataProcessor(
      String databaseEL,
      String tableEL,
      List<PartitionConfig> partition_list,
      boolean externalTable,
      String tablePathTemplate,
      String partitionPathTemplate,
      HiveConfigBean hiveConfig,
      String timeDriver
  ) {
    this.databaseEL = databaseEL;
    this.tableEL = tableEL;
    this.externalTable = externalTable;
    this.partitionConfigList = partition_list;
    this.hiveConfigBean = hiveConfig;
    if (externalTable){
      this.tablePathTemplate = tablePathTemplate;
      this.partitionPathTemplate = partitionPathTemplate;
    }
    this.timeDriver = timeDriver;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues =  super.init();

    hiveConfigBean.init(getContext(), "hiveConfigBean", issues);

    if (!externalTable) {
      internalWarehouseDir = HiveConf.getVar(hiveConfigBean.getConfiguration(), HiveConf.ConfVars.METASTOREWAREHOUSE);
      if (internalWarehouseDir == null || internalWarehouseDir.isEmpty()){
        issues.add(getContext().createConfigIssue(
            Groups.HIVE.name(),
            "warehouseDirectory",
            Errors.HIVE_METADATA_01,
            "Warehouse Directory"));
      }
    }

    if (partitionConfigList.isEmpty()) {
      issues.add(getContext().createConfigIssue(
          Groups.HIVE.name(),
          "partitionList",
          Errors.HIVE_METADATA_01,
          "Partition Configuration"));
    }

    partitionTypeInfo = new LinkedHashMap<>();
    for (PartitionConfig partition: partitionConfigList){
      // Validation on partition column name
      if(!HiveMetastoreUtil.validateColumnName(partition.name)){
        issues.add(getContext().createConfigIssue(
            Groups.HIVE.name(),
            "partitionList",
            Errors.HIVE_METADATA_04,
            "Partition Configuration"));
        continue;
      }
      // Expression for partition value is not automatically checked on Preview
      if (partition.valueEL.isEmpty()){
        issues.add(getContext().createConfigIssue(
            Groups.HIVE.name(),
            "partitionList",
            Errors.HIVE_METADATA_02,
            "Partition Configuration",
            partition.name));
        continue;
      }
      partitionTypeInfo.put(
          partition.name,
          partition.typeConfig.valueType.getSupport().createTypeInfo(partition.typeConfig)
      );
    }

    if (issues.isEmpty()) {
      errorRecordHandler = new DefaultErrorRecordHandler(getContext());
      elEvals.init(getContext());
      try {
        HiveMetastoreUtil.getTimeBasis(
            getContext(),
            getContext().createRecord("validationConfigs"),
            timeDriver,
            elEvals.timeDriverElEval
        );
      } catch (ELEvalException ex) {
        issues.add(
            getContext().createConfigIssue(
                Groups.ADVANCED.name(),
                "conf.timeDriver",
                Errors.HIVE_METADATA_05,
                ex.toString(),
                ex
            )
        );
      }
      hdfsLane = getContext().getOutputLanes().get(0);
      hmsLane = getContext().getOutputLanes().get(1);
      // load cache
      cache = HMSCache.newCacheBuilder()
          .addCacheTypeSupport(
              Arrays.asList(
                  HMSCacheType.TBLPROPERTIES_INFO,
                  HMSCacheType.TYPE_INFO,
                  HMSCacheType.PARTITION_VALUE_INFO,
                  HMSCacheType.AVRO_SCHEMA_INFO
              )
          )
          .maxCacheSize(hiveConfigBean.maxCacheSize)
          .build();
    }
    return issues;
  }

  @Override
  protected void process(Record record, BatchMaker batchMaker) throws StageException {
    ELVars variables = getContext().createELVars();
    RecordEL.setRecordInContext(variables, record);
    TimeEL.setCalendarInContext(variables, Calendar.getInstance());

    String dbName = HiveMetastoreUtil.resolveEL(elEvals.dbNameELEval, variables, databaseEL);
    String tableName = HiveMetastoreUtil.resolveEL(elEvals.tableNameELEval,variables,tableEL);
    String warehouseDir;
    if (externalTable){
      warehouseDir = HiveMetastoreUtil.resolveEL(elEvals.tablePathTemplateELEval, variables,tablePathTemplate);
    } else {
      warehouseDir = internalWarehouseDir;
    }
    LinkedHashMap<String, String> partitionValMap = new LinkedHashMap<>();
    String avroSchema;
    boolean schemaChanged = false;

    // First, find out if this record has all necessary data to process
    try {
      if (dbName.isEmpty()) {
        dbName = DEFAULT_DB;
      }
      validateNames(dbName, tableName, warehouseDir);
      String qualifiedName = HiveMetastoreUtil.getQualifiedTableName(dbName, tableName);
      // path from warehouse directory to table
      String targetPath = HiveMetastoreUtil.getTargetDirectory(warehouseDir, dbName, tableName);
      // Obtain the record structure from current record
      LinkedHashMap<String, HiveTypeInfo> recordStructure = HiveMetastoreUtil.convertRecordToHMSType(record);
      // Obtain all the partition values from record and build a path using partition values
      String partitionStr = getPartitionValuesFromRecord(record, variables, partitionValMap);

      TBLPropertiesInfoCacheSupport.TBLPropertiesInfo tblPropertiesInfo =
          (TBLPropertiesInfoCacheSupport.TBLPropertiesInfo) getCacheInfo(
              HMSCacheType.TBLPROPERTIES_INFO,
              qualifiedName
          );
      if (tblPropertiesInfo != null && tblPropertiesInfo.isExternal() != externalTable) {
        throw new HiveStageCheckedException(
            com.streamsets.pipeline.stage.lib.hive.Errors.HIVE_23,
            "EXTERNAL",
            externalTable,
            tblPropertiesInfo.isExternal()
        );
      }

      TypeInfoCacheSupport.TypeInfo tableCache
          = (TypeInfoCacheSupport.TypeInfo) getCacheInfo(HMSCacheType.TYPE_INFO, qualifiedName);

      AvroSchemaInfoCacheSupport.AvroSchemaInfo schemaCache
          = (AvroSchemaInfoCacheSupport.AvroSchemaInfo) getCacheInfo(HMSCacheType.AVRO_SCHEMA_INFO, qualifiedName);

      // Generate schema only if there is no table exist, or schema is changed.
      if (tableCache == null || detectSchemaChange(recordStructure,tableCache)) {
        schemaChanged = true;
        avroSchema = HiveMetastoreUtil.generateAvroSchema(recordStructure, qualifiedName);
        handleSchemaChange(record, dbName, tableName, recordStructure, targetPath,
            avroSchema, batchMaker, qualifiedName, tableCache, schemaCache);
      } else {
        if (schemaCache == null) { // Table exists in Hive, but this is cold start so the cache is null
          avroSchema = HiveMetastoreUtil.generateAvroSchema(recordStructure, qualifiedName);
          updateAvroCache(schemaCache, avroSchema, qualifiedName);
        } else  // No schema change, table already exists in Hive, and we have avro schema in cache.
          avroSchema = schemaCache.getSchema();
      }

      // Send new partition metadata if new partition is detected.
      PartitionInfoCacheSupport.PartitionInfo pCache
          = (PartitionInfoCacheSupport.PartitionInfo) getCacheInfo(HMSCacheType.PARTITION_VALUE_INFO, qualifiedName);
      Set<LinkedHashMap<String, String>> diff = detectNewPartition(partitionValMap, pCache);

      // Append partition path to target path as all paths from now should be with the partition info
      targetPath += partitionStr;

      if (diff != null) {
        handleNewPartition(partitionValMap, pCache, record, dbName, tableName, targetPath, batchMaker, qualifiedName, diff);
      }

      // Send record to HDFS target
      updateRecordForHDFS(record, schemaChanged, avroSchema, targetPath);
      batchMaker.addRecord(record, hdfsLane);
    } catch (HiveStageCheckedException error) {
      LOG.error("Error happened when processing the record : {}" + record.toString(), error);
      errorRecordHandler.onError(new OnRecordErrorException(record, error.getErrorCode(), error.getParams()));
    }
  }

  private void validateNames(String dbName, String tableName, String warehouseDir)
      throws HiveStageCheckedException {

    if (!HiveMetastoreUtil.validateName(dbName)){
      throw new HiveStageCheckedException(Errors.HIVE_METADATA_04, "Database name", dbName);
    }
    if (tableName.isEmpty()) {
      throw new HiveStageCheckedException(Errors.HIVE_METADATA_03, tableEL);
    } else if (!HiveMetastoreUtil.validateName(tableName)){
      throw new HiveStageCheckedException(Errors.HIVE_METADATA_04, HIVE_TABLE_NAME, tableName);
    }
    if (warehouseDir.isEmpty()) {
      throw new HiveStageCheckedException(Errors.HIVE_METADATA_03, warehouseDir);
    }
  }
  /**
   * Get cached data from cache. First call getIfPresent to obtain data from local cache.
   * If not exist, load from HMS
   * @param cacheType Type of caache to load.
   * @param qualifiedName  Qualified name. E.g. "default.sampleTable"
   * @return Cache object if successfully loaded. Null if no data is found in cache.
   * @throws StageException
   */
  private HMSCacheSupport.HMSCacheInfo getCacheInfo(HMSCacheType cacheType, String qualifiedName)
      throws StageException {
    HMSCacheSupport.HMSCacheInfo cacheInfo;

    cacheInfo = cache.getIfPresent(  // Or better to keep this in this class?
        cacheType,
        qualifiedName);

    if (cacheType != HMSCacheType.AVRO_SCHEMA_INFO && cacheInfo == null) {
      // Try loading by executing HMS query
      cacheInfo = cache.getOrLoad(
          cacheType,
          hiveConfigBean.hiveJDBCUrl,
          qualifiedName,
          hiveConfigBean.getUgi()
      );
    }
    return cacheInfo;
  }

  // ------------ Handle New Schema ------------------------//
  @VisibleForTesting
  boolean detectSchemaChange(
      LinkedHashMap<String, HiveTypeInfo> recordStructure,
      TypeInfoCacheSupport.TypeInfo cache) throws StageException
  {
    LinkedHashMap<String, HiveTypeInfo> columnDiff = null;
    // compare the record structure vs cache
    if (cache != null) {
      columnDiff = cache.getDiff(recordStructure);
    }
    return columnDiff != null && !columnDiff.isEmpty();
  }

  @VisibleForTesting
  Record generateSchemaChangeRecord(
      Record record,
      String database,
      String tableName,
      LinkedHashMap<String, HiveTypeInfo> columnList,
      LinkedHashMap<String, HiveTypeInfo> partitionTypeList,
      String location,
      String avroSchema
  ) throws HiveStageCheckedException
  {
    Record metadataRecord = getContext().cloneRecord(record);

    Field metadataField = HiveMetastoreUtil.newSchemaMetadataFieldBuilder(
        database,
        tableName,
        columnList,
        partitionTypeList,
        !externalTable,  // need to flip boolean to send if this is internal table
        location,
        avroSchema
    );
    metadataRecord.set(metadataField);
    return metadataRecord;
  }

  private void handleSchemaChange(
      Record record,
      String dbName,
      String tableName,
      LinkedHashMap<String, HiveTypeInfo> recordStructure,
      String targetDir, String avroSchema,
      BatchMaker batchMaker,
      String qualifiedName,
      TypeInfoCacheSupport.TypeInfo tableCache,
      AvroSchemaInfoCacheSupport.AvroSchemaInfo schemaCache)
      throws StageException {

    Record r = generateSchemaChangeRecord(record, dbName, tableName, recordStructure, partitionTypeInfo, targetDir, avroSchema);
    batchMaker.addRecord(r, hmsLane);
    // update or insert the new record structure to cache
    if (tableCache != null) {
      tableCache.updateState(recordStructure);
    } else {
      cache.put(
          HMSCacheType.TYPE_INFO,
          qualifiedName,
          new TypeInfoCacheSupport.TypeInfo(recordStructure, partitionTypeInfo)
      );
    }
    updateAvroCache(schemaCache, avroSchema, qualifiedName);
  }

  // -------- Handle New Partitions ------------------//

  /**
   * Using partition name and value that were obtained from record, compare them
   * with cached partition.
   * @param partitionValMap List of partition name and value found in Record
   * @param pCache  Cache that has existing partitions
   * @return Diff of partitions if new partition is detected. Otherwise null.
   * @throws StageException
   */
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
    }
    return null;
  }

  String getPartitionValue(Date date, Record record, String partitionValueEL) throws ELEvalException {
    ELVars vars = getContext().createELVars();
    RecordEL.setRecordInContext(vars, record);
    if (date != null) {
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      TimeEL.setCalendarInContext(vars, calendar);
    }
    return HiveMetastoreUtil.resolveEL(elEvals.partitionValueELEval, vars, partitionValueEL);
  }

  /**
   * Obtain a list of partition values from record.
   * @param variables ELvariables
   * @param values Blank LinkedHashMap. This function fills parition name and value obtained from record.
   * @return String that represents partitions name=value.
   *         For example, "dt=2016-01-01/country=US/state=CA"
   * @throws StageException
   */
  @VisibleForTesting
  String getPartitionValuesFromRecord(Record r, ELVars variables, LinkedHashMap<String, String> values)
      throws StageException
  {
    StringBuilder sb = new StringBuilder();
    Date timeBasis = HiveMetastoreUtil.getTimeBasis(getContext(), r, timeDriver, elEvals.timeDriverElEval);
    for (PartitionConfig pName: partitionConfigList) {
      String ret = getPartitionValue(timeBasis, r, pName.valueEL);
      if (ret == null || ret.isEmpty()) {
        // If no partition value is found in record, this record goes to Error Record
        throw new HiveStageCheckedException(Errors.HIVE_METADATA_03, pName.valueEL);
      }  else {
        values.put(pName.name, ret);
        sb.append(HiveMetastoreUtil.SEP);
        sb.append(pName.name);
        sb.append(HiveMetastoreUtil.EQUALS);
        sb.append(ret);
      }
    }
    return sb.toString();
  }

  /**
   * Generate a record for new partition. It creates a new Record
   * and fill in metadata.
   * @param database database name
   * @param tableName table name
   * @param partitionList New partition to be created
   * @param location Direcotry path
   * @return New metadata record
   * @throws StageException
   */
  @VisibleForTesting
  Record generateNewPartitionRecord(
      Record record,
      String database,
      String tableName,
      LinkedHashMap<String, String> partitionList,
      String location) throws StageException {

    Record metadataRecord = getContext().cloneRecord(record);
    Field metadataField = HiveMetastoreUtil.newPartitionMetadataFieldBuilder(
        database,
        tableName,
        partitionList,
        location
    );
    metadataRecord.set(metadataField);
    return metadataRecord;
  }

  private void handleNewPartition(
      LinkedHashMap<String, String> partitionValMap,
      PartitionInfoCacheSupport.PartitionInfo pCache,
      Record record,
      String database,
      String tableName,
      String location,
      BatchMaker batchMaker,
      String qualifiedName,
      Set<LinkedHashMap<String, String>> diff
  ) throws StageException{

    Record r = generateNewPartitionRecord(record, database, tableName, partitionValMap, location);
    batchMaker.addRecord(r, hmsLane);
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

  private void updateAvroCache(AvroSchemaInfoCacheSupport.AvroSchemaInfo avroCache, String newState, String qualifiedName)
      throws StageException {

    if (avroCache != null) {
      avroCache.updateState(newState);
    } else {
      cache.put(
          HMSCacheType.AVRO_SCHEMA_INFO,
          qualifiedName,
          new AvroSchemaInfoCacheSupport.AvroSchemaInfo(newState)
      );
    }
  }

  //Add header information to send to HDFS
  @VisibleForTesting
  static void updateRecordForHDFS(
      Record record,
      boolean roll,
      String avroSchema,
      String location
  ){
    if(roll){
      record.getHeader().setAttribute(HDFS_HEADER_ROLL, "true");
    }
    record.getHeader().setAttribute(HDFS_HEADER_AVROSCHEMA, avroSchema);
    record.getHeader().setAttribute(HDFS_HEADER_TARGET_DIRECTORY, location);
  }
}
