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
package com.streamsets.pipeline.stage.lib.hive;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.lib.hive.cache.PartitionInfoCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.exceptions.HiveStageCheckedException;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveType;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveTypeInfo;
import com.streamsets.pipeline.stage.processor.hive.HMPDataFormat;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Helper for executing JDBC Hive Queries.
 */
public final class HiveQueryExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(HiveQueryExecutor.class);
  private static final String ALTER_TABLE = "ALTER TABLE %s";
  private static final String CREATE_TABLE = "CREATE %s TABLE %s";
  private static final String DESC = "DESC %s";
  private static final String DESC_FORMATTED = "DESC formatted %s ";
  private static final String DESC_FORMATTED_PARTITION = DESC_FORMATTED +"partition";
  private static final String DESCRIBE_DATABASE = "DESCRIBE DATABASE `%s`";
  private static final String SHOW_TABLES = "SHOW TABLES in %s like '%s'";
  private static final String PARTITIONED_BY = "PARTITIONED BY";
  private static final String ADD_COLUMNS = "ADD COLUMNS";
  private static final String ADD_PARTITION = "ADD PARTITION";
  private static final String SHOW_PARTITIONS = "SHOW PARTITIONS %s";
  private static final String SHOW_TBLPROPERTIES = "SHOW TBLPROPERTIES %s";
  private static final String PARTITION_FIELD_EQUALS_NON_QUOTES_VAL = "`%s`=%s";
  private static final String PARTITION_FIELD_EQUALS_QUOTES_VAL = "`%s`='%s'";
  private static final String TBL_PROPERTIES = "TBLPROPERTIES";
  private static final String AVRO_SCHEMA_URL = "avro.schema.url";
  private static final String STORED_AS_AVRO = "STORED AS AVRO";
  private static final String STORED_AS_PARQUET = "STORED AS PARQUET";
  private static final String OLD_WAY_AVRO_ROW_STORAGE_INPUT_OUPTUT_FORMAT =
      " ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'" +
          " STORED AS" +
          " INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'" +
          " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'";
  private static final String LOCATION = "LOCATION";
  private static final String SET = "SET";
  private static final String EXTERNAL = "EXTERNAL";

  private static final String RESULT_SET_COL_NAME = "col_name";
  private static final String RESULT_SET_DATA_TYPE = "data_type";
  private static final String RESULT_SET_PROP_NAME = "prpt_name";
  private static final String RESULT_SET_PROP_VALUE = "prpt_value";
  private static final String RESULT_SET_LOCATION = "location";
  private static final String LOCATION_INFORMATION_IN_RESULT_SET = "Location:";
  private static final String DETAILED_PARTITION_INFORMATION = "# Detailed Partition Information";
  private static final String SERDE_LIBRARY_IN_RESULT_SET = "SerDe Library:";
  private static final String STORAGE_INFORMATION = "# Storage Information";
  private final HiveConfigBean hiveConfigBean;
  private final Meter selectMeter;
  private final Timer selectTimer;
  private final Meter updateMeter;
  private final Timer updateTimer;

  public HiveQueryExecutor(HiveConfigBean hiveConfigBean, Stage.Context context) {
    this.hiveConfigBean = hiveConfigBean;

    this.selectMeter = context.createMeter("Select Queries");
    this.selectTimer = context.createTimer("Select Queries");
    this.updateMeter = context.createMeter("Update Queries");
    this.updateTimer = context.createTimer("Update Queries");
  }

  // Internal interface for wrapping work that needs to happen with given result set
  private interface WithResultSet<T> {
    public T run(ResultSet rs) throws SQLException, StageException;
  }

  private static void buildNameTypeFormatWithElements(
      StringBuilder sb,
      LinkedHashMap<String, HiveTypeInfo> linkedHashMap
  ) {
    boolean needComma = false;
    for (Map.Entry<String, HiveTypeInfo> keyVal : linkedHashMap.entrySet()) {
      if (needComma) {
        sb.append(HiveMetastoreUtil.COMMA);
      }
      String columnName = keyVal.getKey();
      HiveTypeInfo hiveTypeInfo = keyVal.getValue();
      sb.append(
          hiveTypeInfo.getHiveType().getSupport().generateColumnTypeDefinition(
              hiveTypeInfo, columnName
          )
      );
      needComma = true;
    }
  }

  private static void buildPartitionNameValuePair(
      StringBuilder sb,
      LinkedHashMap<String, String> partitionValueMap,
      Map<String, HiveTypeInfo> partitionTypeMap
  ) {
    boolean needComma = false;
    for (Map.Entry<String, String> partitionValEntry : partitionValueMap.entrySet()) {
      if (needComma) {
        sb.append(HiveMetastoreUtil.COMMA);
      }
      //Even INT/BIG_INT partition values work with quotes so won't be a problem i use
      //string to generate the partitiond definition.
      //This is only used for desc extended table name partition (partition definition) query
      HiveType partitionType = (partitionTypeMap.containsKey(partitionValEntry.getKey()))?
          partitionTypeMap.get(partitionValEntry.getKey()).getHiveType() : HiveType.STRING;
      String format = (partitionType == HiveType.STRING)?
          PARTITION_FIELD_EQUALS_QUOTES_VAL: PARTITION_FIELD_EQUALS_NON_QUOTES_VAL;
      sb.append(String.format(format, partitionValEntry.getKey(), partitionValEntry.getValue()));
      needComma = true;
    }
  }


  private static String buildAvroSchemaTableProperty(String schemaPath) {
    StringBuilder sb = new StringBuilder();
    sb.append(HiveMetastoreUtil.SINGLE_QUOTE);
    sb.append(AVRO_SCHEMA_URL);
    sb.append(HiveMetastoreUtil.SINGLE_QUOTE);
    sb.append(HiveMetastoreUtil.EQUALS);
    sb.append(HiveMetastoreUtil.SINGLE_QUOTE);
    sb.append(schemaPath);
    sb.append(HiveMetastoreUtil.SINGLE_QUOTE);
    return sb.toString();
  }

  private static void buildCreateTableQuery(
      StringBuilder sb,
      String qualifiedTableName,
      LinkedHashMap<String, HiveTypeInfo> columnTypeMap,
      LinkedHashMap<String, HiveTypeInfo> partitionTypeMap,
      boolean isInternal
  ) {
    sb.append(String.format(CREATE_TABLE, isInternal? "": EXTERNAL, qualifiedTableName));
    sb.append(HiveMetastoreUtil.SPACE);

    sb.append(HiveMetastoreUtil.OPEN_BRACKET);
    buildNameTypeFormatWithElements(sb, columnTypeMap);
    sb.append(HiveMetastoreUtil.CLOSE_BRACKET);

    sb.append(HiveMetastoreUtil.SPACE);
    if (!partitionTypeMap.isEmpty()) {
      sb.append(PARTITIONED_BY);

      sb.append(HiveMetastoreUtil.OPEN_BRACKET);
      buildNameTypeFormatWithElements(sb, partitionTypeMap);
      sb.append(HiveMetastoreUtil.CLOSE_BRACKET);
    }
  }

  private static String buildCreateTableQueryNew(
      String qualifiedTableName,
      String location,
      LinkedHashMap<String, HiveTypeInfo> columnTypeMap,
      LinkedHashMap<String, HiveTypeInfo> partitionTypeMap,
      boolean isInternal,
      HMPDataFormat dataFormat
  ) {
    StringBuilder sb = new StringBuilder();
    buildCreateTableQuery(sb, qualifiedTableName, columnTypeMap, partitionTypeMap, isInternal);
    sb.append(HiveMetastoreUtil.SPACE);
    //Stored as AVRO used for new way of creating a table.
    if (dataFormat == HMPDataFormat.PARQUET) {
      sb.append(STORED_AS_PARQUET);
    } else if (dataFormat == HMPDataFormat.AVRO){
      sb.append(STORED_AS_AVRO);
    }
    sb.append(HiveMetastoreUtil.SPACE);
    sb.append(LOCATION);
    sb.append(HiveMetastoreUtil.SPACE);
    sb.append(HiveMetastoreUtil.SINGLE_QUOTE);
    sb.append(location);
    sb.append(HiveMetastoreUtil.SINGLE_QUOTE);
    return sb.toString();
  }

  private static String buildCreateTableQueryOld(
      String qualifiedTableName,
      String location,
      LinkedHashMap<String, HiveTypeInfo> columnTypeMap,
      LinkedHashMap<String, HiveTypeInfo> partitionTypeMap,
      String schemaPath,
      boolean isInternal
  ) {
    StringBuilder sb = new StringBuilder();
    buildCreateTableQuery(sb, qualifiedTableName, columnTypeMap, partitionTypeMap, isInternal);
    sb.append(HiveMetastoreUtil.SPACE);
    sb.append(OLD_WAY_AVRO_ROW_STORAGE_INPUT_OUPTUT_FORMAT);
    sb.append(HiveMetastoreUtil.SPACE);
    sb.append(LOCATION);
    sb.append(HiveMetastoreUtil.SPACE);
    sb.append(HiveMetastoreUtil.SINGLE_QUOTE);
    sb.append(location);
    sb.append(HiveMetastoreUtil.SINGLE_QUOTE);
    sb.append(TBL_PROPERTIES);
    sb.append(HiveMetastoreUtil.OPEN_BRACKET);
    sb.append(buildAvroSchemaTableProperty(schemaPath));
    sb.append(HiveMetastoreUtil.CLOSE_BRACKET);
    return sb.toString();
  }

  private static String buildAddColumnsQuery(
      String qualifiedTableName,
      LinkedHashMap<String, HiveTypeInfo> columnTypeMap
  ) {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format(ALTER_TABLE, qualifiedTableName));
    sb.append(HiveMetastoreUtil.SPACE);
    sb.append(ADD_COLUMNS);
    sb.append(HiveMetastoreUtil.SPACE);
    sb.append(HiveMetastoreUtil.OPEN_BRACKET);
    buildNameTypeFormatWithElements(sb, columnTypeMap);
    sb.append(HiveMetastoreUtil.CLOSE_BRACKET);
    return sb.toString();
  }

  private static String buildPartitionAdditionQuery(
      String qualifiedTableName,
      LinkedHashMap<String, String> partitionColumnValMap,
      Map<String, HiveTypeInfo> partitionTypeMap,
      String partitionPath
  ) {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format(ALTER_TABLE, qualifiedTableName));
    sb.append(HiveMetastoreUtil.SPACE);
    sb.append(ADD_PARTITION);
    sb.append(HiveMetastoreUtil.SPACE);
    sb.append(HiveMetastoreUtil.OPEN_BRACKET);
    buildPartitionNameValuePair(sb, partitionColumnValMap, partitionTypeMap);
    sb.append(HiveMetastoreUtil.CLOSE_BRACKET);
    sb.append(HiveMetastoreUtil.SPACE);
    if (partitionPath != null) {
      sb.append(LOCATION);
      sb.append(HiveMetastoreUtil.SPACE);
      sb.append(HiveMetastoreUtil.SINGLE_QUOTE);
      sb.append(partitionPath);
      sb.append(HiveMetastoreUtil.SINGLE_QUOTE);
    }
    return sb.toString();
  }

  private static String buildDescExtendedPartitionQuery(
      String qualifiedTableName,
      LinkedHashMap<String, String> partitionValues
  ) {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format(DESC_FORMATTED_PARTITION, qualifiedTableName));
    sb.append(HiveMetastoreUtil.SPACE);
    sb.append(HiveMetastoreUtil.OPEN_BRACKET);
    //Empty for partition types
    //This is so that cache loader for partition values does
    //not have to depend on type info
    buildPartitionNameValuePair(sb, partitionValues, new LinkedHashMap<String, HiveTypeInfo>());
    sb.append(HiveMetastoreUtil.CLOSE_BRACKET);
    return sb.toString();
  }

  private static String buildSetTablePropertiesQuery(
      String qualifiedTableName,
      String schemaPath
  ) {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format(ALTER_TABLE, qualifiedTableName));
    sb.append(HiveMetastoreUtil.SPACE);
    sb.append(SET);
    sb.append(HiveMetastoreUtil.SPACE);
    sb.append(TBL_PROPERTIES);
    sb.append(HiveMetastoreUtil.SPACE);
    sb.append(HiveMetastoreUtil.OPEN_BRACKET);
    sb.append(buildAvroSchemaTableProperty(schemaPath));
    sb.append(HiveMetastoreUtil.CLOSE_BRACKET);
    return sb.toString();
  }

  private static String buildShowPartitionsQuery(String qualifiedTableName) {
    return String.format(SHOW_PARTITIONS, qualifiedTableName);
  }

  private static String buildDescTableQuery(String qualifiedTableName) {
    return String.format(DESC, qualifiedTableName);
  }

  private static String buildDescribeDatabase(String dbName) {
    return String.format(DESCRIBE_DATABASE, dbName);
  }

  @VisibleForTesting
  public static String buildShowTableQuery(String qualifiedTableName) {
    String[] dbTable = qualifiedTableName.split("\\.");
    String db = dbTable[0];
    // The table name will be used inside string constant rather then as object name and hence we need to de-escape it
    String table = dbTable[1].replace("`", "");
    return String.format(SHOW_TABLES, db, table);
  }


  public boolean executeShowTableQuery(String qualifiedTableName) throws StageException{
    String sql = buildShowTableQuery(qualifiedTableName);

    return executeQuery(sql, new WithResultSet<Boolean>() {
      @Override
      public Boolean run(ResultSet rs) throws SQLException, StageException {
        return rs.next();
      }
    });
  }

  public void executeCreateTableQuery(
      String qualifiedTableName,
      String tableLocation,
      LinkedHashMap<String, HiveTypeInfo> columnTypeMap,
      LinkedHashMap<String, HiveTypeInfo> partitionTypeMap,
      boolean useAsAvro,
      String schemaLocation,
      boolean isInternal,
      HMPDataFormat dataFormat
  ) throws StageException {
    String sql = null;
    if (dataFormat == HMPDataFormat.PARQUET) {
      sql = buildCreateTableQueryNew(qualifiedTableName, tableLocation, columnTypeMap, partitionTypeMap, isInternal, dataFormat);
    } else {

      Utils.checkArgument((useAsAvro || schemaLocation != null),
          "Invalid configuration for table creation in use As Avro"
      );
      sql = useAsAvro ? buildCreateTableQueryNew(qualifiedTableName,
          tableLocation,
          columnTypeMap,
          partitionTypeMap,
          isInternal,
          dataFormat
      ) : buildCreateTableQueryOld(qualifiedTableName,
          tableLocation,
          columnTypeMap,
          partitionTypeMap,
          schemaLocation,
          isInternal
      );
    }
    execute(sql);
  }

  public void executeAlterTableAddColumnsQuery(
      String qualifiedTableName,
      LinkedHashMap<String, HiveTypeInfo> columnTypeMap
  ) throws StageException {
    String sql = buildAddColumnsQuery(qualifiedTableName, columnTypeMap);
    execute(sql);
  }

  /**
   * Add a new partition to the given table, with optional custom location.
   * @param qualifiedTableName Table name in the form `database`.`table`.
   * @param partitionNameValueMap Map between partition column names and their values.
   * @param partitionTypeMap Map between partition column names and their types.
   * @param partitionPath Location in the Hadoop filesystem where the partition will be created. If null, the default
   *                      location will be used.
   */
  public void executeAlterTableAddPartitionQuery(
      String qualifiedTableName,
      LinkedHashMap<String, String> partitionNameValueMap,
      Map<String, HiveTypeInfo> partitionTypeMap,
      String partitionPath
  ) throws StageException {
    String sql = buildPartitionAdditionQuery(qualifiedTableName, partitionNameValueMap, partitionTypeMap, partitionPath);
    execute(sql);
  }

  /**
   * Execute Alter Table set Table Properties
   * @param qualifiedTableName qualified table name.
   * @param partitionPath parition path.
   * @throws StageException in case of any {@link SQLException}
   */
  public void executeAlterTableSetTblPropertiesQuery(
      String qualifiedTableName,
      String partitionPath
  ) throws StageException {
    String sql = buildSetTablePropertiesQuery(qualifiedTableName, partitionPath);
    execute(sql);
  }

  /**
   * Returns {@link Set} of partitions
   * @param qualifiedTableName qualified table name
   * @return {@link Set} of partitions
   * @throws StageException in case of any {@link SQLException}
   */
  public Set<PartitionInfoCacheSupport.PartitionValues> executeShowPartitionsQuery(String qualifiedTableName) throws StageException {
    String sql = buildShowPartitionsQuery(qualifiedTableName);

    return executeQuery(sql, new WithResultSet<Set<PartitionInfoCacheSupport.PartitionValues>>() {
      @Override
      public Set<PartitionInfoCacheSupport.PartitionValues> run(ResultSet rs) throws SQLException, StageException {
        Set<PartitionInfoCacheSupport.PartitionValues> partitionValuesSet = new HashSet<>();
        while(rs.next()) {
          String partitionInfoString = rs.getString(1);
          String[] partitionInfoSplit = partitionInfoString.split(HiveMetastoreUtil.SEP);
          LinkedHashMap<String, String> vals = new LinkedHashMap<>();
          for (String partitionValInfo : partitionInfoSplit) {
            String[] partitionNameVal = partitionValInfo.split("=");
            vals.put(partitionNameVal[0], partitionNameVal[1]);
          }
          partitionValuesSet.add(new PartitionInfoCacheSupport.PartitionValues(vals));
        }
        return partitionValuesSet;
      }
    });
  }

  @VisibleForTesting
  protected Pair<LinkedHashMap<String, HiveTypeInfo>, LinkedHashMap<String, HiveTypeInfo>> extractTypeInfo(ResultSet rs)
      throws StageException {
    LinkedHashMap<String, HiveTypeInfo> columnTypeInfo = new LinkedHashMap<>();
    Map<String, HiveTypeInfo> temporaryMap = new LinkedHashMap<>();

    boolean processedColumnInfo = false;

    try {
      while (rs.next()) {
        String columnName = rs.getString(RESULT_SET_COL_NAME);
        if (columnName == null) {
          break;
        }
        if (columnName.startsWith("#") || columnName.isEmpty()) {
          // If we found a delimiter we just skip it
          if (!processedColumnInfo && !temporaryMap.isEmpty()) {
            //After the delimiters we stop processing column info and start with partition info
            processedColumnInfo = true;
            columnTypeInfo.putAll(temporaryMap);
            temporaryMap = new LinkedHashMap<>();
          }
        } else {
          String columnTypeString = rs.getString(RESULT_SET_DATA_TYPE);
          HiveTypeInfo hiveTypeInfo = HiveType.prefixMatch(columnTypeString)
                                              .getSupport()
                                              .generateHiveTypeInfoFromResultSet(columnTypeString);
          temporaryMap.put(columnName, hiveTypeInfo);
        }
      }
    } catch (SQLException e) {
      LOG.error("SQL Exception: " + e.getMessage() + " {}", e);
      throw new HiveStageCheckedException(Errors.HIVE_20, "", e.getMessage());
    }

    LinkedHashMap<String, HiveTypeInfo> partitionTypeInfo;

    if (columnTypeInfo.isEmpty()) {
      //If we do not have any column type information add it, and let partition info empty
      columnTypeInfo.putAll(temporaryMap);
      partitionTypeInfo = new LinkedHashMap<>();
    } else {
      partitionTypeInfo = new LinkedHashMap<>(temporaryMap);
    }

    // At this point columnTypeInfo field, depending on hive version,
    // can potentially contain column names + partition names
    // Before returning, make sure that you dont double count partition information in column information
    // Meaning, return a pair like so .. Pair<strictly column information, strictly partition information>
    Map<String, HiveTypeInfo> strictlyColumnInfo = Maps.difference(columnTypeInfo, partitionTypeInfo)
        .entriesOnlyOnLeft();

    return Pair.of(new LinkedHashMap<>(strictlyColumnInfo), partitionTypeInfo);
  }

  /**
   * Returns {@link Pair} of Column Type Info and Partition Type Info.
   *
   * @param qualifiedTableName qualified table name.
   * @return {@link Pair} of Column Type Info and Partition Type Info.
   *
   * @throws StageException in case of any {@link SQLException}
   */
  public Pair<LinkedHashMap<String, HiveTypeInfo>, LinkedHashMap<String, HiveTypeInfo>> executeDescTableQuery(
      String qualifiedTableName
  ) throws StageException {
    String sql = buildDescTableQuery(qualifiedTableName);

    return executeQuery(sql, this::extractTypeInfo);
  }

  /**
   * Returns location for given database.
   *
   * @param dbName Database name
   * @return Path where the database is stored
   * @throws StageException in case of any {@link SQLException}
   */
  public String executeDescribeDatabase(String dbName) throws StageException {
    String sql = buildDescribeDatabase(dbName);

    return executeQuery(sql, rs -> {
      if(!rs.next()) {
        throw new HiveStageCheckedException(Errors.HIVE_35, "Database doesn't exists.");
      }

      return HiveMetastoreUtil.stripHdfsHostAndPort(rs.getString(RESULT_SET_LOCATION));
    });
  }

  public String executeDescFormattedExtractSerdeLibrary(
      final String qualifiedTableName
  ) throws StageException {
    final String sql = String.format(DESC_FORMATTED, qualifiedTableName);

    return executeQuery(sql, new WithResultSet<String>() {
      @Override
      public String run(ResultSet rs) throws SQLException, StageException {
        String serdeLibrary = null;
        boolean isStorageInfoSeen = false;
        while (rs.next()) {
          String col_name = rs.getString(RESULT_SET_COL_NAME).trim();
          if (col_name.equals(STORAGE_INFORMATION)) {
            isStorageInfoSeen = true;
          }
          if (isStorageInfoSeen && col_name.startsWith(SERDE_LIBRARY_IN_RESULT_SET)) {
            serdeLibrary = rs.getString(RESULT_SET_DATA_TYPE);
            break;
          }
        }
        if (serdeLibrary == null) {
          throw new HiveStageCheckedException(
              Errors.HIVE_20,
              sql,
              Utils.format("Serde Library not found for table {}", qualifiedTableName)
          );
        }
        return serdeLibrary;
      }
    });
  }

  public String executeDescFormattedPartitionAndGetLocation(
      final String qualifiedTableName,
      LinkedHashMap<String, String> partitionValues
  ) throws StageException {
    final String sql = buildDescExtendedPartitionQuery(qualifiedTableName, partitionValues);

    return executeQuery(sql, new WithResultSet<String>() {
      @Override
      public String run(ResultSet rs) throws SQLException, StageException {
        String location = null;
        boolean isDetailedPartitionInformationRowSeen = false;
        while (rs.next()) {
          String col_name = rs.getString(RESULT_SET_COL_NAME).trim();
          if (col_name.equals(DETAILED_PARTITION_INFORMATION)) {
            isDetailedPartitionInformationRowSeen = true;
          }
          if (isDetailedPartitionInformationRowSeen
              && col_name != null
              && col_name.startsWith(LOCATION_INFORMATION_IN_RESULT_SET)) {
            //Replace hdfs://host:port
            location = HiveMetastoreUtil.stripHdfsHostAndPort(rs.getString(RESULT_SET_DATA_TYPE));
            break;
          }
        }
        if (location == null) {
          throw new HiveStageCheckedException(
              Errors.HIVE_20,
              sql,
              Utils.format("Location information not found for partitions in table {}", qualifiedTableName)
          );
        }
        return location;
      }
    });
  }

  /**
   * Returns {@link Pair} of IsExternal and storedAsAvro TBLProperties.
   * @param qualifiedTableName qualified table name.
   * @return {@link Pair} of IsExternal and storedAsAvro.
   * @throws StageException in case of any {@link SQLException}
   */
  public Pair<Boolean, Boolean> executeShowTBLPropertiesQuery(
      String qualifiedTableName
  ) throws StageException {
    String sql = String.format(SHOW_TBLPROPERTIES, qualifiedTableName);

    return executeQuery(sql, new WithResultSet<Pair<Boolean, Boolean>>() {
      @Override
      public Pair<Boolean, Boolean> run(ResultSet rs) throws SQLException {
        boolean isExternal = false, useAsAvro = true;

        while (rs.next()) {
          String propName = rs.getString(RESULT_SET_PROP_NAME);
          String propValue = rs.getString(RESULT_SET_PROP_VALUE);
          if (propName.toUpperCase().equals(EXTERNAL)) {
            isExternal = Boolean.valueOf(propValue);
          } else if (propName.equals(AVRO_SCHEMA_URL)) {
            useAsAvro = false;
          }
        }
        return Pair.of(isExternal, useAsAvro);
      }
    });
  }

  /**
   * Execute given query.
   *
   * With all required side effects such as catching exceptions, updating metrics, ...
   * @param query Query to execute
   * @throws StageException
   */
  private void execute(String query) throws StageException {
    LOG.debug("Executing SQL: {}", query);
    Timer.Context t = updateTimer.time();
    try(Statement statement = hiveConfigBean.getHiveConnection().createStatement()) {
      statement.execute(query);
    } catch (Exception e) {
      LOG.error("Exception while processing query: {}", query, e);
      throw new HiveStageCheckedException(Errors.HIVE_20, query, e.getMessage());
    } finally {
      long time = t.stop();
      LOG.debug("Query '{}' took {} nanoseconds", query, time);
      updateMeter.mark();
    }
  }

  /**
   * Execute given query and process it's result set.
   *
   * With all required side effects such as catching exceptions, updating metrics, ...
   *
   * @param query Query to execute
   * @param execution Action that should be carried away on the result set
   * @throws StageException
   */
  private<T> T executeQuery(String query, WithResultSet<T> execution) throws StageException {
    LOG.debug("Executing SQL:  {}", query);
    Timer.Context t = selectTimer.time();
    try(
      Statement statement = hiveConfigBean.getHiveConnection().createStatement();
      ResultSet rs = statement.executeQuery(query);
    ) {
      // Stop timer immediately so that we're calculating only query execution time and not the processing time
      long time = t.stop();
      LOG.debug("Query '{}' took {} nanoseconds", query, time);
      t = null;

      return execution.run(rs);
    } catch(Exception e) {
      LOG.error("Exception while processing query: {}", query, e);
      throw new HiveStageCheckedException(Errors.HIVE_20, query, e.getMessage());
    } finally {
      // If the timer wasn't stopped due to exception yet, stop it now
      if(t != null) {
        long time = t.stop();
        LOG.debug("Query '{}' took {} nanoseconds", query, time);
      }
      selectMeter.mark();
    }
  }
}
