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
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.RecordProcessor;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.lib.hive.HiveConfigBean;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;
import com.streamsets.pipeline.stage.lib.hive.Groups;
import com.streamsets.pipeline.stage.lib.hive.HiveQueryExecutor;
import com.streamsets.pipeline.stage.lib.hive.cache.AvroSchemaInfoCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.cache.HMSCache;
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
  private static final String SCALE_EXPRESSION = "scaleExpression";
  private static final String PRECISION_EXPRESSION = "precisionExpression";

  private static final String WAREHOUSE_DIR_PROPERTY = "hive.metastore.warehouse.dir";
  protected static final String HDFS_HEADER_ROLL = "roll";
  protected static final String HDFS_HEADER_AVROSCHEMA = "avroSchema";
  protected static final String HDFS_HEADER_TARGET_DIRECTORY = "targetDirectory";
  public static final String DEFAULT_DB = "default";
  private final String databaseEL;
  private final String tableEL;
  private final boolean externalTable;
  private final String timeDriver;
  private final DecimalDefaultsConfig decimalDefaultsConfig;

  private HiveConfigBean hiveConfigBean;
  private String internalWarehouseDir;

  private boolean partitioned;

  // Triplet of partition name, type and value expression obtained from configuration
  private List<PartitionConfig> partitionConfigList;
  // List of partition name and value type.
  private LinkedHashMap<String, HiveTypeInfo> partitionTypeInfo;

  // Optional configuration values
  private String tablePathTemplate;
  private String partitionPathTemplate;

  private String hdfsLane;
  private String hmsLane;
  private HMSCache cache;
  private ErrorRecordHandler errorRecordHandler;
  private HiveMetadataProcessorELEvals elEvals = new HiveMetadataProcessorELEvals();

  private static class HiveMetadataProcessorELEvals {
    private ELEval dbNameELEval;
    private ELEval tableNameELEval;
    private ELEval partitionValueELEval;
    private ELEval tablePathTemplateELEval;
    private ELEval timeDriverElEval;
    private ELEval partitionPathTemplateELEval;
    private ELEval scaleEL;
    private ELEval precisionEL;

    public void init(Stage.Context context) {
      dbNameELEval = context.createELEval(HIVE_DB_NAME);
      tableNameELEval = context.createELEval(HIVE_TABLE_NAME);
      partitionValueELEval = context.createELEval(HIVE_PARTITION_CONFIG_VALUE_EL);
      tablePathTemplateELEval = context.createELEval(TABLE_PATH_TEMPLATE);
      partitionPathTemplateELEval = context.createELEval(PARTITION_PATH_TEMPLATE);
      timeDriverElEval = context.createELEval(TIME_DRIVER);
      scaleEL = context.createELEval(SCALE_EXPRESSION);
      precisionEL = context.createELEval(PRECISION_EXPRESSION);
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
      String timeDriver,
      DecimalDefaultsConfig decimalDefaultsConfig
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
    this.decimalDefaultsConfig = decimalDefaultsConfig;
    partitionTypeInfo = new LinkedHashMap<>();
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues =  super.init();
    hiveConfigBean.init(getContext(), "hiveConfigBean", issues);
    partitioned = !(partitionConfigList.isEmpty());
    if (partitioned) {
      PartitionColumnTypeChooserValues supportedPartitionTypes = new PartitionColumnTypeChooserValues();
      for (PartitionConfig partition: partitionConfigList) {
        // Validation on partition column name
        if (!HiveMetastoreUtil.validateColumnName(partition.name)) {
          issues.add(getContext().createConfigIssue(
              Groups.HIVE.name(),
              "partitionList",
              Errors.HIVE_METADATA_03,
              "Partition Configuration",
              partition.name
          ));
        }

        // Validate that the partition type is indeed supported
        if(!supportedPartitionTypes.getValues().contains(partition.valueType.name())) {
          issues.add(getContext().createConfigIssue(
            Groups.HIVE.name(),
            "partitionList",
            Errors.HIVE_METADATA_09,
            partition.valueType));
        }

        // Expression for partition value is not automatically checked on Preview
        if (partition.valueEL.isEmpty()) {
          issues.add(getContext().createConfigIssue(
              Groups.HIVE.name(),
              "partitionList",
              Errors.HIVE_METADATA_01,
              "Partition Configuration",
              partition.name));
        }
        partitionTypeInfo.put(
            partition.name.toLowerCase(),
            partition.valueType.getSupport().createTypeInfo(partition.valueType)
        );
      }
    }

    if (!externalTable) {
      internalWarehouseDir
          = new HiveConf(hiveConfigBean.getConfiguration(), HiveConf.class)
          .get(WAREHOUSE_DIR_PROPERTY);
      validateTemplate(internalWarehouseDir, "Hive Warehouse directory", Errors.HIVE_METADATA_05, issues);
    } else {
      validateTemplate(tablePathTemplate, "Table Path Template", Errors.HIVE_METADATA_06, issues);
      if (partitioned)
        validateTemplate(partitionPathTemplate, "Partition Path Template", Errors.HIVE_METADATA_06, issues);
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
                Errors.HIVE_METADATA_04,
                ex.toString(),
                ex
            )
        );
      }
      hdfsLane = getContext().getOutputLanes().get(0);
      hmsLane = getContext().getOutputLanes().get(1);
      // load cache
      try {
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
            .build(new HiveQueryExecutor(hiveConfigBean.getHiveConnection()));
      } catch (StageException e) {
        issues.add(getContext().createConfigIssue(
            Groups.HIVE.name(),
            "hiveConfigBean.hiveJDBCUrl",
            com.streamsets.pipeline.stage.lib.hive.Errors.HIVE_01,
            e.getMessage()
        ));
      }
    }
    return issues;
  }

  @Override
  public void destroy() {
    hiveConfigBean.destroy();
    super.destroy();
  }

  private void validateTemplate(String template, String configName, ErrorCode errorCode, List<ConfigIssue> issues){
    if (template == null || template.isEmpty()){
      issues.add(getContext().createConfigIssue(
          Groups.HIVE.name(),
          template,
          errorCode,
          configName
      ));
    }
  }

  private void changeRecordFieldToLowerCase(Record record) {
    Field field = record.get();
    Map<String, Field> newFieldMap = new LinkedHashMap<>();
    for (Map.Entry<String, Field> fieldEntry : field.getValueAsMap().entrySet()) {
      //Use toLowercase on fieldName so as to have column/partition name lower case.
      newFieldMap.put(fieldEntry.getKey().toLowerCase(), fieldEntry.getValue());
    }
    record.set(Field.create(newFieldMap));
  }

  @Override
  protected void process(Record record, BatchMaker batchMaker) throws StageException {
    ELVars variables = getContext().createELVars();
    RecordEL.setRecordInContext(variables, record);
    TimeEL.setCalendarInContext(variables, Calendar.getInstance());
    String dbName = HiveMetastoreUtil.resolveEL(elEvals.dbNameELEval, variables, databaseEL);
    String tableName = HiveMetastoreUtil.resolveEL(elEvals.tableNameELEval,variables,tableEL);
    String warehouseDir, avroSchema;
    String partitionStr = "";
    LinkedHashMap<String, String> partitionValMap;
    boolean schemaChanged = false;

    if (dbName.isEmpty()) {
      dbName = DEFAULT_DB;
    }
    try{
      partitionValMap = getPartitionValuesFromRecord(record);
      warehouseDir = externalTable ?
          HiveMetastoreUtil.resolveEL(elEvals.tablePathTemplateELEval, variables, tablePathTemplate) :
          internalWarehouseDir;

      if (partitioned) {
        partitionStr = externalTable ?
            HiveMetastoreUtil.resolveEL(elEvals.partitionPathTemplateELEval, variables, partitionPathTemplate) :
            HiveMetastoreUtil.generatePartitionPath(partitionValMap);
        if (!partitionStr.startsWith("/"))
          partitionStr = "/" + partitionStr;
      }
      // First, find out if this record has all necessary data to process
      validateNames(dbName, tableName, warehouseDir);
      String qualifiedName = HiveMetastoreUtil.getQualifiedTableName(dbName, tableName);
      // path from warehouse directory to table
      String targetPath = externalTable ? warehouseDir :
          HiveMetastoreUtil.getTargetDirectory(warehouseDir, dbName, tableName);

      // Obtain the record structure from current record
      LinkedHashMap<String, HiveTypeInfo> recordStructure = HiveMetastoreUtil.convertRecordToHMSType(
          record,
          elEvals.scaleEL,
          elEvals.precisionEL,
          decimalDefaultsConfig.scaleExpression,
          decimalDefaultsConfig.precisionExpression,
          variables
      );

      if (recordStructure.isEmpty()) {  // If record has no data to process, No-op
        return;
      }

      TBLPropertiesInfoCacheSupport.TBLPropertiesInfo tblPropertiesInfo = HiveMetastoreUtil.getCacheInfo(
          cache,
          HMSCacheType.TBLPROPERTIES_INFO,
          qualifiedName
      );

      if (tblPropertiesInfo != null) {
        if (!tblPropertiesInfo.getSerdeLibrary().equals(HiveMetastoreUtil.AVRO_SERDE)) {
          throw new HiveStageCheckedException(
              com.streamsets.pipeline.stage.lib.hive.Errors.HIVE_32,
              qualifiedName,
              tblPropertiesInfo.getSerdeLibrary()
          );
        }
        if (tblPropertiesInfo != null && tblPropertiesInfo.isExternal() != externalTable) {
          throw new HiveStageCheckedException(
              com.streamsets.pipeline.stage.lib.hive.Errors.HIVE_23,
              "EXTERNAL",
              externalTable,
              tblPropertiesInfo.isExternal()
          );
        }
      }

      TypeInfoCacheSupport.TypeInfo tableCache = HiveMetastoreUtil.getCacheInfo(
          cache,
          HMSCacheType.TYPE_INFO,
          qualifiedName
      );

      if (tableCache != null) {
        //Checks number and name of partitions.
        HiveMetastoreUtil.validatePartitionInformation(tableCache, partitionValMap, qualifiedName);
        //Checks the type of partitions.
        Map<String, HiveTypeInfo> cachedPartitionTypeInfoMap = tableCache.getPartitionTypeInfo();
        for (Map.Entry<String, HiveTypeInfo> cachedPartitionTypeInfo : cachedPartitionTypeInfoMap.entrySet()) {
          String partitionName = cachedPartitionTypeInfo.getKey();
          HiveTypeInfo expectedTypeInfo = cachedPartitionTypeInfo.getValue();
          HiveTypeInfo actualTypeInfo = partitionTypeInfo.get(partitionName);
          if (!expectedTypeInfo.equals(actualTypeInfo)) {
            throw new HiveStageCheckedException(
                com.streamsets.pipeline.stage.lib.hive.Errors.HIVE_28,
                partitionName,
                qualifiedName,
                expectedTypeInfo.toString(),
                actualTypeInfo.toString()
            );
          }
        }
      }

      AvroSchemaInfoCacheSupport.AvroSchemaInfo schemaCache = HiveMetastoreUtil.getCacheInfo(
          cache,
          HMSCacheType.AVRO_SCHEMA_INFO,
          qualifiedName
      );

      // Generate schema only if there is no table exist, or schema is changed.
      if (tableCache == null || detectSchemaChange(recordStructure,tableCache)) {
        schemaChanged = true;
        avroSchema = HiveMetastoreUtil.generateAvroSchema(recordStructure, qualifiedName);
        handleSchemaChange(dbName, tableName, recordStructure, targetPath,
            avroSchema, batchMaker, qualifiedName, tableCache, schemaCache);
      } else {
        if (schemaCache == null) { // Table exists in Hive, but this is cold start so the cache is null
          avroSchema = HiveMetastoreUtil.generateAvroSchema(recordStructure, qualifiedName);
          updateAvroCache(schemaCache, avroSchema, qualifiedName);
        } else  // No schema change, table already exists in Hive, and we have avro schema in cache.
          avroSchema = schemaCache.getSchema();
      }

      if (partitioned) {
        PartitionInfoCacheSupport.PartitionInfo pCache = HiveMetastoreUtil.getCacheInfo(
            cache,
            HMSCacheType.PARTITION_VALUE_INFO,
            qualifiedName
        );

        // Append partition path to target path as all paths from now should be with the partition info
        targetPath += partitionStr;

        Map<PartitionInfoCacheSupport.PartitionValues, String> diff = detectNewPartition(partitionValMap, pCache, targetPath);

        // Send new partition metadata if new partition is detected.
        if (diff != null) {
          handleNewPartition(partitionValMap, pCache, dbName, tableName, targetPath, batchMaker, qualifiedName, diff);
        }
      }
      // Send record to HDFS target.
      changeRecordFieldToLowerCase(record);
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
      throw new HiveStageCheckedException(Errors.HIVE_METADATA_03, HIVE_DB_NAME, dbName);
    }
    if (tableName.isEmpty()) {
      throw new HiveStageCheckedException(Errors.HIVE_METADATA_02, tableEL);
    } else if (!HiveMetastoreUtil.validateName(tableName)){
      throw new HiveStageCheckedException(Errors.HIVE_METADATA_03, HIVE_TABLE_NAME, tableName);
    }
    if (warehouseDir.isEmpty()) {
      throw new HiveStageCheckedException(Errors.HIVE_METADATA_02, warehouseDir);
    }
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
      String database,
      String tableName,
      LinkedHashMap<String, HiveTypeInfo> columnList,
      LinkedHashMap<String, HiveTypeInfo> partitionTypeList,
      String location,
      String avroSchema
  ) throws HiveStageCheckedException
  {
    Record metadataRecord = getContext().createRecord("Table Metadata Record");

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
      String dbName,
      String tableName,
      LinkedHashMap<String, HiveTypeInfo> recordStructure,
      String targetDir, String avroSchema,
      BatchMaker batchMaker,
      String qualifiedName,
      TypeInfoCacheSupport.TypeInfo tableCache,
      AvroSchemaInfoCacheSupport.AvroSchemaInfo schemaCache)
      throws StageException {

    Record r = generateSchemaChangeRecord(dbName, tableName, recordStructure, partitionTypeInfo, targetDir, avroSchema);
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
  private Map<PartitionInfoCacheSupport.PartitionValues, String> detectNewPartition(
      LinkedHashMap<String, String> partitionValMap,
      PartitionInfoCacheSupport.PartitionInfo pCache,
      String location
  ) throws StageException{
    // Start evaluating partition value
    PartitionInfoCacheSupport.PartitionValues partitionValues =
        new PartitionInfoCacheSupport.PartitionValues(partitionValMap);
    Map<PartitionInfoCacheSupport.PartitionValues, String> partitionInfoDiff = new HashMap<>();
    partitionInfoDiff.put(partitionValues, location);

    partitionInfoDiff
        = (pCache != null)? pCache.getDiff(partitionInfoDiff) : partitionInfoDiff;
    if (pCache == null || !partitionInfoDiff.isEmpty()){
      return partitionInfoDiff;
    }
    return null;
  }

  @VisibleForTesting
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
   * @return LinkedHashMap that contains pairs of partition name-values
   * @throws StageException
   */
  @VisibleForTesting
  LinkedHashMap<String, String> getPartitionValuesFromRecord(Record r)
      throws StageException
  {
    LinkedHashMap<String, String> values = new LinkedHashMap<>();
    Date timeBasis = HiveMetastoreUtil.getTimeBasis(getContext(), r, timeDriver, elEvals.timeDriverElEval);
    for (PartitionConfig pName: partitionConfigList) {
      String ret = getPartitionValue(timeBasis, r, pName.valueEL);
      if (ret == null || ret.isEmpty()) {
        // If no partition value is found in record, this record goes to Error Record
        throw new HiveStageCheckedException(Errors.HIVE_METADATA_02, pName.valueEL);
      }  else if (HiveMetastoreUtil.hasUnsupportedChar(ret)){
          throw new HiveStageCheckedException(Errors.HIVE_METADATA_10, pName.valueEL, ret);
      }
      values.put(pName.name.toLowerCase(), ret);
    }
    return values;
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
      String database,
      String tableName,
      LinkedHashMap<String, String> partitionList,
      String location) throws StageException {

    Record metadataRecord = getContext().createRecord("Partition Metadata Record");
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
      String database,
      String tableName,
      String location,
      BatchMaker batchMaker,
      String qualifiedName,
      Map<PartitionInfoCacheSupport.PartitionValues, String> diff
  ) throws StageException{

    Record r = generateNewPartitionRecord(database, tableName, partitionValMap, location);
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
