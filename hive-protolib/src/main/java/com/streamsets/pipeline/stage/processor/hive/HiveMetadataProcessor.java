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
package com.streamsets.pipeline.stage.processor.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
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
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.lib.hive.HiveConfigBean;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;
import com.streamsets.pipeline.stage.lib.hive.HiveQueryExecutor;
import com.streamsets.pipeline.stage.lib.hive.cache.AvroSchemaInfoCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.cache.HMSCache;
import com.streamsets.pipeline.stage.lib.hive.cache.HMSCacheType;
import com.streamsets.pipeline.stage.lib.hive.cache.PartitionInfoCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.cache.TBLPropertiesInfoCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.cache.TypeInfoCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.exceptions.HiveStageCheckedException;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveTypeInfo;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

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
  private static final String COMMENT_EXPRESSION = "commentExpression";
  private static final String TEMP_AVRO_DIR_NAME = "/.avro";
  private static final String ATTRIBUTE_EXPRESSION = "metadataHeaderAttributeConfigs";
  private static final String KEY_HMS_CACHE = "hms-cache";

  protected static final String HDFS_HEADER_ROLL = "roll";
  protected static final String HDFS_HEADER_AVROSCHEMA = "avroSchema";
  protected static final String HDFS_HEADER_TARGET_DIRECTORY = "targetDirectory";


  public static final String DEFAULT_DB = "default";
  private final String databaseEL;
  private final String tableEL;
  private final boolean externalTable;
  private final String timeDriver;
  private final DecimalDefaultsConfig decimalDefaultsConfig;
  private final TimeZone timeZone;
  private final HMPDataFormat dataFormat;
  private final String commentExpression;
  private final Map<String, String> metadataHeaderAttributeConfigs;

  private boolean metadataHeadersToAddExist;

  private HiveConfigBean hiveConfigBean;

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

  // Database cache only holds metadata for databases
  private LoadingCache<String, String> databaseCache;

  // The HMS cache is holding all info about tables
  private HMSCache cache;

  private ErrorRecordHandler errorRecordHandler;
  private HiveMetadataProcessorELEvals elEvals = new HiveMetadataProcessorELEvals();
  private HiveQueryExecutor queryExecutor;

  private static class HiveMetadataProcessorELEvals {
    private ELEval dbNameELEval;
    private ELEval tableNameELEval;
    private ELEval partitionValueELEval;
    private ELEval tablePathTemplateELEval;
    private ELEval timeDriverElEval;
    private ELEval partitionPathTemplateELEval;
    private ELEval scaleEL;
    private ELEval precisionEL;
    private ELEval commentEL;
    private ELEval metadataHeaderAttributeEL;

    public void init(Stage.Context context) {
      dbNameELEval = context.createELEval(HIVE_DB_NAME);
      tableNameELEval = context.createELEval(HIVE_TABLE_NAME);
      partitionValueELEval = context.createELEval(HIVE_PARTITION_CONFIG_VALUE_EL);
      tablePathTemplateELEval = context.createELEval(TABLE_PATH_TEMPLATE);
      partitionPathTemplateELEval = context.createELEval(PARTITION_PATH_TEMPLATE);
      timeDriverElEval = context.createELEval(TIME_DRIVER);
      scaleEL = context.createELEval(SCALE_EXPRESSION);
      precisionEL = context.createELEval(PRECISION_EXPRESSION);
      commentEL = context.createELEval(COMMENT_EXPRESSION);
      metadataHeaderAttributeEL = context.createELEval(ATTRIBUTE_EXPRESSION);
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
      DecimalDefaultsConfig decimalDefaultsConfig,
      TimeZone timezone,
      HMPDataFormat dataFormat,
      String commentExpression,
      Map<String, String> metadataHeaderAttributeConfigs
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
    this.timeZone = timezone;
    this.dataFormat = dataFormat;
    this.commentExpression = commentExpression;
    this.metadataHeaderAttributeConfigs = metadataHeaderAttributeConfigs;
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
        if (!HiveMetastoreUtil.validateObjectName(partition.name)) {
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
            partition.valueType.getSupport().createTypeInfo(partition.valueType, "")
        );
      }
    }

    if (externalTable) {
      validateTemplate(tablePathTemplate, "Table Path Template", Errors.HIVE_METADATA_06, issues);
      if (partitioned)
        validateTemplate(partitionPathTemplate, "Partition Path Template", Errors.HIVE_METADATA_06, issues);
    }

    if (issues.isEmpty()) {
      errorRecordHandler = new DefaultErrorRecordHandler(getContext());
      elEvals.init(getContext());
      try {
        elEvals.timeDriverElEval.eval(getContext().createELVars(), timeDriver, Date.class);
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
        // We have exactly one instance of the query executor per stage to calculate it's metrics
        queryExecutor = new HiveQueryExecutor(hiveConfigBean, getContext());

        Map<String, Object> runnerSharedMap = getContext().getStageRunnerSharedMap();
        synchronized (runnerSharedMap) {
          if(runnerSharedMap.containsKey(KEY_HMS_CACHE)) {
            cache = (HMSCache) runnerSharedMap.get(KEY_HMS_CACHE);
          } else {
            cache = HMSCache.newCacheBuilder()
              .maxCacheSize(hiveConfigBean.maxCacheSize)
              .addCacheTypeSupport(ImmutableList.of(
                HMSCacheType.TBLPROPERTIES_INFO,
                HMSCacheType.TYPE_INFO,
                HMSCacheType.PARTITION_VALUE_INFO,
                HMSCacheType.AVRO_SCHEMA_INFO
              )).build();

            runnerSharedMap.put(KEY_HMS_CACHE, cache);
          }
        }

        databaseCache = CacheBuilder
          .newBuilder()
          .maximumSize(50)
          .build(new CacheLoader<String, String>() {
              @Override
              public String load(String dbName) throws Exception {
                return queryExecutor.executeDescribeDatabase(dbName);
              }
            });
      } catch (StageException e) {
        issues.add(getContext().createConfigIssue(
            Groups.HIVE.name(),
            "hiveConfigBean.hiveJDBCUrl",
            com.streamsets.pipeline.stage.lib.hive.Errors.HIVE_01,
            e.getMessage()
        ));
      }

      if(!metadataHeaderAttributeConfigs.isEmpty()) {
        metadataHeadersToAddExist = true;
        for (Map.Entry<String, String> entry : metadataHeaderAttributeConfigs.entrySet()) {
          String attributeNameExpression = entry.getKey();
          String attributeValueExpression = entry.getValue();

          ELUtils.validateExpression(
                  elEvals.metadataHeaderAttributeEL,
                  getContext().createELVars(),
                  attributeNameExpression,
                  getContext(),
                  ATTRIBUTE_EXPRESSION,
                  "headerAttributeConfigs",
                  Errors.HIVE_METADATA_12,
                  Object.class,
                  issues);
          ELUtils.validateExpression(
                  elEvals.metadataHeaderAttributeEL,
                  getContext().createELVars(),
                  attributeValueExpression,
                  getContext(),
                  ATTRIBUTE_EXPRESSION,
                  "headerAttributeConfigs",
                  Errors.HIVE_METADATA_12,
                  Object.class,
                  issues);
        }
      } else {
        metadataHeadersToAddExist = false;
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
    TimeNowEL.setTimeNowInContext(variables, new Date());

    // Calculate record time for this particular record and persist it in the variables
    Date timeBasis = elEvals.timeDriverElEval.eval(variables, timeDriver, Date.class);
    Calendar calendar = Calendar.getInstance(timeZone);
    calendar.setTime(timeBasis);
    TimeEL.setCalendarInContext(variables, calendar);

    String dbName = HiveMetastoreUtil.resolveEL(elEvals.dbNameELEval, variables, databaseEL);
    String tableName = HiveMetastoreUtil.resolveEL(elEvals.tableNameELEval,variables,tableEL);
    String targetPath;
    String avroSchema;
    String partitionStr = "";
    LinkedHashMap<String, String> partitionValMap;

    if (dbName.isEmpty()) {
      dbName = DEFAULT_DB;
    }
    try {
      // Validate Database and Table names
      if (!HiveMetastoreUtil.validateObjectName(dbName)) {
        throw new HiveStageCheckedException(Errors.HIVE_METADATA_03, "database name", dbName);
      }
      if (!HiveMetastoreUtil.validateObjectName(tableName)) {
        throw new HiveStageCheckedException(Errors.HIVE_METADATA_03, "table name", tableName);
      }

      partitionValMap = getPartitionValuesFromRecord(variables);

      if (partitioned) {
        partitionStr = externalTable ?
            HiveMetastoreUtil.resolveEL(elEvals.partitionPathTemplateELEval, variables, partitionPathTemplate) :
            HiveMetastoreUtil.generatePartitionPath(partitionValMap);
        if (!partitionStr.startsWith("/"))
          partitionStr = "/" + partitionStr;
      }
      // First, find out if this record has all necessary data to process
      validateNames(dbName, tableName);
      String qualifiedName = HiveMetastoreUtil.getQualifiedTableName(dbName, tableName);
      LOG.trace("Generated table {} for record {}", qualifiedName, record.getHeader().getSourceId());

      if(externalTable) {
        // External table have location in the resolved EL
        targetPath = HiveMetastoreUtil.resolveEL(elEvals.tablePathTemplateELEval, variables, tablePathTemplate);
      } else {
        // Internal table will be the database location + table name
        String databaseLocation;
        try {
          databaseLocation = databaseCache.get(dbName);
        } catch (ExecutionException e) {
          throw new HiveStageCheckedException(com.streamsets.pipeline.stage.lib.hive.Errors.HIVE_23, e.getMessage());
        }
        targetPath = String.format("%s/%s", databaseLocation, tableName);
      }

      if (targetPath.isEmpty()) {
        throw new HiveStageCheckedException(Errors.HIVE_METADATA_02, targetPath);
      }

        // Obtain the record structure from current record
      LinkedHashMap<String, HiveTypeInfo> recordStructure = HiveMetastoreUtil.convertRecordToHMSType(
          record,
          elEvals.scaleEL,
          elEvals.precisionEL,
          elEvals.commentEL,
          decimalDefaultsConfig.scaleExpression,
          decimalDefaultsConfig.precisionExpression,
          commentExpression,
          variables
      );

      if (recordStructure.isEmpty()) {  // If record has no data to process, No-op
        return;
      }

      TBLPropertiesInfoCacheSupport.TBLPropertiesInfo tblPropertiesInfo = HiveMetastoreUtil.getCacheInfo(
          cache,
          HMSCacheType.TBLPROPERTIES_INFO,
          qualifiedName,
          queryExecutor
      );

      if (tblPropertiesInfo != null) {
        HiveMetastoreUtil.validateTblPropertiesInfo(dataFormat, tblPropertiesInfo, tableName);

        if (tblPropertiesInfo.isExternal() != externalTable) {
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
          qualifiedName,
          queryExecutor
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
        // Validate that the columns from record itself does not clash with partition columns
        for(String columnName : recordStructure.keySet()) {
          if(cachedPartitionTypeInfoMap.containsKey(columnName)) {
             throw new HiveStageCheckedException(
                com.streamsets.pipeline.stage.lib.hive.Errors.HIVE_40,
                columnName
            );
          }
        }
      }

      AvroSchemaInfoCacheSupport.AvroSchemaInfo schemaCache = HiveMetastoreUtil.getCacheInfo(
          cache,
          HMSCacheType.AVRO_SCHEMA_INFO,
          qualifiedName,
          queryExecutor
      );

      // True if there was a schema drift (including detection of new table)
      boolean schemaDrift = false;

      // Build final structure of how the table should look like
      LinkedHashMap<String, HiveTypeInfo> finalStructure;
      if(tableCache != null) {
        // Table already exists in Hive - so it's columns will be preserved and in their original order
        finalStructure = new LinkedHashMap<>();
        finalStructure.putAll(tableCache.getColumnTypeInfo());

        // If there is any diff (any new columns), we will append them at the end of the table
        LinkedHashMap<String, HiveTypeInfo> columnDiff = tableCache.getDiff(recordStructure);
        if(!columnDiff.isEmpty()) {
          LOG.trace("Detected drift for table {} - new columns: {}", qualifiedName, StringUtils.join(columnDiff.keySet(), ","));
          schemaDrift = true;
          finalStructure.putAll(columnDiff);
        }
      } else {
        LOG.trace("{} is a new table", qualifiedName);
        // This table doesn't exists yet, so we'll use record own structure as the final table's structure
        schemaDrift = true;
        finalStructure = recordStructure;
      }

      // Generate schema only if the table do not exist or it's schema is changed.
      if (schemaDrift) {
        avroSchema = HiveMetastoreUtil.generateAvroSchema(finalStructure, qualifiedName);
        LOG.trace("Schema Drift. Generated new Avro schema for table {}: {}", qualifiedName, avroSchema);

        // Add custom metadata attributes if they are specified
        Map<String, String> metadataHeaderAttributeMap = new LinkedHashMap();
        if (metadataHeadersToAddExist) {
          metadataHeaderAttributeMap = generateResolvedHeaderAttributeMap(metadataHeaderAttributeConfigs, variables);
        }

        handleSchemaChange(dbName, tableName, recordStructure, targetPath, avroSchema, batchMaker, qualifiedName, tableCache, schemaCache, metadataHeaderAttributeMap);
      } else {
        if (schemaCache == null) { // Table exists in Hive, but this is cold start so the cache is null
          avroSchema = HiveMetastoreUtil.generateAvroSchema(finalStructure, qualifiedName);
          LOG.trace("Cold Start. Generated new Avro schema for table {}: {}", qualifiedName, avroSchema);
          updateAvroCache(schemaCache, avroSchema, qualifiedName);
        } else  // No schema change, table already exists in Hive, and we have avro schema in cache.
          avroSchema = schemaCache.getSchema();
      }

      if (partitioned) {
        PartitionInfoCacheSupport.PartitionInfo pCache = HiveMetastoreUtil.getCacheInfo(
            cache,
            HMSCacheType.PARTITION_VALUE_INFO,
            qualifiedName,
            queryExecutor
        );

        PartitionInfoCacheSupport.PartitionValues partitionValues = new PartitionInfoCacheSupport.PartitionValues(partitionValMap);

        // If the partition information exist (thus this is not a cold start)
        if(pCache != null) {
          // If we detected drift, we need to persist that information and "roll" all partitions next time
          // we will see them.
          if (schemaDrift) {
            pCache.setAllPartitionsToBeRolled();
          }

          // If we performed drift for the table and this is the firs time we see this partition, we need to
          // set the roll flag anyway.
          if (pCache.shouldRoll(partitionValues)) {
            schemaDrift = true;
          }
        }

        // Append partition path to target path as all paths from now should be with the partition info
        targetPath += partitionStr;

        Map<PartitionInfoCacheSupport.PartitionValues, String> diff = detectNewPartition(partitionValues, pCache, targetPath);

        // Send new partition metadata if new partition is detected.
        if (diff != null) {
          // Add custom metadata attributes if they are specified
          Map<String, String> partitionMetadataHeaderAttributeMap = new LinkedHashMap<>();
          if (metadataHeadersToAddExist) {
            partitionMetadataHeaderAttributeMap = generateResolvedHeaderAttributeMap(metadataHeaderAttributeConfigs, variables);
          }
          handleNewPartition(partitionValMap, pCache, dbName, tableName, targetPath, batchMaker, qualifiedName, diff, partitionMetadataHeaderAttributeMap);
        }
      }

      // Send record to HDFS target.
      if (dataFormat == HMPDataFormat.PARQUET) {
        targetPath = targetPath + TEMP_AVRO_DIR_NAME;
      }

      changeRecordFieldToLowerCase(record);
      updateRecordForHDFS(record, schemaDrift, avroSchema, targetPath);
      batchMaker.addRecord(record, hdfsLane);
    } catch (HiveStageCheckedException error) {
      LOG.error("Error happened when processing record", error);
      LOG.trace("Record that caused the error: {}", record.toString());
      errorRecordHandler.onError(new OnRecordErrorException(record, error.getErrorCode(), error.getParams()));
    }
  }

  private void validateNames(String dbName, String tableName) throws HiveStageCheckedException {
    if (!HiveMetastoreUtil.validateObjectName(dbName)){
      throw new HiveStageCheckedException(Errors.HIVE_METADATA_03, HIVE_DB_NAME, dbName);
    }
    if (tableName.isEmpty()) {
      throw new HiveStageCheckedException(Errors.HIVE_METADATA_02, tableEL);
    } else if (!HiveMetastoreUtil.validateObjectName(tableName)){
      throw new HiveStageCheckedException(Errors.HIVE_METADATA_03, HIVE_TABLE_NAME, tableName);
    }
  }

  @VisibleForTesting
  Record generateSchemaChangeRecord(
      String database,
      String tableName,
      LinkedHashMap<String, HiveTypeInfo> columnList,
      LinkedHashMap<String, HiveTypeInfo> partitionTypeList,
      String location,
      String avroSchema,
      Map<String, String> metadataHeaderAttributes
  ) throws HiveStageCheckedException
  {
    //creating a record with uuid as postfix so multiple SDCs won't generate the record with same id.
    Record metadataRecord = getContext().createRecord("Table Metadata Record" + UUID.randomUUID().toString());

    Field metadataField = HiveMetastoreUtil.newSchemaMetadataFieldBuilder(
        database,
        tableName,
        columnList,
        partitionTypeList,
        !externalTable,  // need to flip boolean to send if this is internal table
        location,
        avroSchema,
        dataFormat
    );
    metadataRecord.set(metadataField);

    for (Map.Entry<String, String> entry : metadataHeaderAttributes.entrySet()){
      metadataRecord.getHeader().setAttribute(entry.getKey(), entry.getValue());
    }

      return metadataRecord;
  }

  private void handleSchemaChange(
      String dbName,
      String tableName,
      LinkedHashMap<String, HiveTypeInfo> recordStructure,
      String targetDir,
      String avroSchema,
      BatchMaker batchMaker,
      String qualifiedName,
      TypeInfoCacheSupport.TypeInfo tableCache,
      AvroSchemaInfoCacheSupport.AvroSchemaInfo schemaCache,
      Map<String, String> metadataHeaderAttributes
  ) throws StageException {
    Record r = generateSchemaChangeRecord(dbName, tableName, recordStructure, partitionTypeInfo, targetDir, avroSchema, metadataHeaderAttributes);
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
   * @param partitionValues Partition representation
   * @param pCache  Cache that has existing partitions
   * @return Diff of partitions if new partition is detected. Otherwise null.
   * @throws StageException
   */
  private Map<PartitionInfoCacheSupport.PartitionValues, String> detectNewPartition(
      PartitionInfoCacheSupport.PartitionValues partitionValues,
      PartitionInfoCacheSupport.PartitionInfo pCache,
      String location
  ) throws StageException{
    Map<PartitionInfoCacheSupport.PartitionValues, String> partitionInfoDiff = new HashMap<>();
    partitionInfoDiff.put(partitionValues, location);

    partitionInfoDiff
        = (pCache != null)? pCache.getDiff(partitionInfoDiff) : partitionInfoDiff;
    if (pCache == null || !partitionInfoDiff.isEmpty()){
      return partitionInfoDiff;
    }
    return null;
  }

  /**
   * Obtain a list of partition values from record.
   * @return LinkedHashMap that contains pairs of partition name-values
   * @throws StageException
   */
  @VisibleForTesting
  LinkedHashMap<String, String> getPartitionValuesFromRecord(ELVars variables) throws StageException {
    LinkedHashMap<String, String> values = new LinkedHashMap<>();
    for (PartitionConfig pName: partitionConfigList) {
      String ret = HiveMetastoreUtil.resolveEL(elEvals.partitionValueELEval, variables, pName.valueEL);
      if (ret == null || ret.isEmpty()) {
        // If no partition value is found in record, this record goes to Error Record
        throw new HiveStageCheckedException(Errors.HIVE_METADATA_02, pName.valueEL);
      } else if (HiveMetastoreUtil.hasUnsupportedChar(ret)){
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
      String location,
      Map<String, String> metadataHeaderAttributes) throws StageException {

    //creating a record with uuid as postfix so multiple SDCs won't generate the record with same id.
    Record metadataRecord = getContext().createRecord("Partition Metadata Record" + UUID.randomUUID().toString());
    Field metadataField = HiveMetastoreUtil.newPartitionMetadataFieldBuilder(
        database,
        tableName,
        partitionList,
        location,
        dataFormat
    );
    metadataRecord.set(metadataField);

    for (Map.Entry<String, String> entry : metadataHeaderAttributes.entrySet()){
      metadataRecord.getHeader().setAttribute(entry.getKey(), entry.getValue());
    }

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
      Map<PartitionInfoCacheSupport.PartitionValues, String> diff,
      Map<String, String> metadataHeaderAttributes
  ) throws StageException {

    Record r = generateNewPartitionRecord(database, tableName, partitionValMap, location, metadataHeaderAttributes);
    batchMaker.addRecord(r, hmsLane);
    if (pCache != null) {
      pCache.updateState(diff);
    } else {
      cache.put(
          HMSCacheType.PARTITION_VALUE_INFO,
          qualifiedName,
          new PartitionInfoCacheSupport.PartitionInfo(diff, queryExecutor, qualifiedName)
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
    LOG.trace("Record {} will be stored in {} path: roll({}), avro schema: {}", record.getHeader().getSourceId(), location, roll, avroSchema);
  }

  private Map<String, String> generateResolvedHeaderAttributeMap(Map<String, String> metadataHeaderAttributeConfigs, ELVars variables) throws ELEvalException {

    Map<String, String> resultMap = new LinkedHashMap();
    for (Map.Entry<String, String> entry : metadataHeaderAttributeConfigs.entrySet()) {
      String attributeNameExpression = entry.getKey();
      String nameResult = HiveMetastoreUtil.resolveEL(elEvals.metadataHeaderAttributeEL, variables, attributeNameExpression);
      if (nameResult.isEmpty()) {
        continue;
      }
      String attributeValueExpression = entry.getValue();
      String valueResult = HiveMetastoreUtil.resolveEL(elEvals.metadataHeaderAttributeEL, variables, attributeValueExpression);
      resultMap.put(nameResult, valueResult);
    }
    return resultMap;

  }
}
