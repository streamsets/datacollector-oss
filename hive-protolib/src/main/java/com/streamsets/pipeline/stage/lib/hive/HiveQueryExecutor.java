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
package com.streamsets.pipeline.stage.lib.hive;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.destination.hive.Errors;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
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
  private static final String SHOW_TABLES = "SHOW TABLES in %s like '%s'";
  private static final String PARTITIONED_BY = "PARTITIONED BY";
  private static final String ADD_COLUMNS = "ADD COLUMNS";
  private static final String ADD_PARTITION = "ADD PARTITION";
  private static final String SHOW_PARTITIONS = "SHOW PARTITIONS %s";
  private static final String COLUMN_TYPE = "%s %s";
  private static final String PARTITION_FIELD_EQUALS_NON_QUOTES_VAL = "%s=%s";
  private static final String PARTITION_FIELD_EQUALS_QUOTES_VAL = "%s='%s'";
  private static final Set<HiveType> QUOTES_HIVE_TYPES =
      ImmutableSet.of(HiveType.CHAR, HiveType.STRING, HiveType.DATE);
  private static final String TBL_PROPERTIES = "TBLPROPERTIES";
  private static final String AVRO_SCHEMA_URL = "avro.schema.url";
  private static final String STORED_AS_AVRO = "STORED AS AVRO";
  private static final String LOCATION = "LOCATION";
  private static final String SET = "SET";
  private static final String EXTERNAL = "EXTERNAL";
  private static final String OPEN_BRACKET = "(";
  private static final String CLOSE_BRACKET = ")";
  private static final String COMMA = ",";
  private static final String SPACE = " ";
  private static final String SINGLE_QUOTE = "'";

  private String jdbcUrl;

  public HiveQueryExecutor(String resolvedJDBCUrl) {
    this.jdbcUrl = resolvedJDBCUrl;
  }

  private static void buildNameTypeFormatWithElements(
      StringBuilder sb,
      LinkedHashMap<String, HiveType> linkedHashMap
  ) {
    boolean needComma = false;
    for (Map.Entry<String, HiveType> keyVal : linkedHashMap.entrySet()) {
      if (needComma) {
        sb.append(COMMA);
      }
      sb.append(String.format(COLUMN_TYPE, keyVal.getKey(), keyVal.getValue().name()));
      needComma = true;
    }
  }

  private static void buildPartitionNameValuePair(
      StringBuilder sb,
      LinkedHashMap<String, String> partitionValueMap,
      Map<String, HiveType> partitionTypeMap
  ) {
    boolean needComma = false;
    for (Map.Entry<String, String> partitionValEntry : partitionValueMap.entrySet()) {
      if (needComma) {
        sb.append(COMMA);
      }
      HiveType partitionType = partitionTypeMap.get(partitionValEntry.getKey());
      String format = (QUOTES_HIVE_TYPES.contains(partitionType))?
          PARTITION_FIELD_EQUALS_QUOTES_VAL: PARTITION_FIELD_EQUALS_NON_QUOTES_VAL;
      sb.append(String.format(format, partitionValEntry.getKey(), partitionValEntry.getValue()));
      needComma = true;
    }
  }


  private static String buildAvroSchemaTableProperty(String schemaPath) {
    StringBuilder sb = new StringBuilder();
    sb.append(SINGLE_QUOTE);
    sb.append(AVRO_SCHEMA_URL);
    sb.append(SINGLE_QUOTE);
    sb.append(HiveMetastoreUtil.EQUALS);
    sb.append(SINGLE_QUOTE);
    sb.append(schemaPath);
    sb.append(SINGLE_QUOTE);
    return sb.toString();
  }

  private static void buildCreateTableQuery(
      StringBuilder sb,
      String qualifiedTableName,
      LinkedHashMap<String, HiveType> columnTypeMap,
      LinkedHashMap<String, HiveType> partitionTypeMap,
      boolean isInternal
  ) {
    sb.append(String.format(CREATE_TABLE, isInternal? "": EXTERNAL, qualifiedTableName));
    sb.append(SPACE);

    sb.append(OPEN_BRACKET);
    buildNameTypeFormatWithElements(sb, columnTypeMap);
    sb.append(CLOSE_BRACKET);

    sb.append(SPACE);
    sb.append(PARTITIONED_BY);

    sb.append(OPEN_BRACKET);
    buildNameTypeFormatWithElements(sb, partitionTypeMap);
    sb.append(CLOSE_BRACKET);
  }

  private static String buildCreateTableQueryNew(
      String qualifiedTableName,
      LinkedHashMap<String, HiveType> columnTypeMap,
      LinkedHashMap<String, HiveType> partitionTypeMap,
      boolean isInternal
  ) {
    StringBuilder sb = new StringBuilder();
    buildCreateTableQuery(sb, qualifiedTableName, columnTypeMap, partitionTypeMap, isInternal);
    sb.append(SPACE);
    //Stored as AVRO used for new way of creating a table.
    sb.append(STORED_AS_AVRO);
    return sb.toString();
  }

  private static String buildCreateTableQueryOld(
      String qualifiedTableName,
      LinkedHashMap<String, HiveType> columnTypeMap,
      LinkedHashMap<String, HiveType> partitionTypeMap,
      String schemaPath,
      boolean isInternal
  ) {
    StringBuilder sb = new StringBuilder();
    buildCreateTableQuery(sb, qualifiedTableName, columnTypeMap, partitionTypeMap, isInternal);

    //TODO: check
    sb.append(SPACE);
    sb.append(TBL_PROPERTIES);
    sb.append(OPEN_BRACKET);
    sb.append(buildAvroSchemaTableProperty(schemaPath));
    sb.append(CLOSE_BRACKET);
    return sb.toString();
  }

  private static String buildAddColumnsQuery(
      String qualifiedTableName,
      LinkedHashMap<String, HiveType> columnTypeMap
  ) {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format(ALTER_TABLE, qualifiedTableName));
    sb.append(SPACE);
    sb.append(ADD_COLUMNS);
    sb.append(SPACE);
    sb.append(OPEN_BRACKET);
    buildNameTypeFormatWithElements(sb, columnTypeMap);
    sb.append(CLOSE_BRACKET);
    return sb.toString();
  }

  private static String buildPartitionAdditionQuery(
      String qualifiedTableName,
      LinkedHashMap<String, String> partitionColumnValMap,
      Map<String, HiveType> partitionTypeMap,
      String partitionPath
  ) {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format(ALTER_TABLE, qualifiedTableName));
    sb.append(SPACE);
    sb.append(ADD_PARTITION);
    sb.append(SPACE);
    sb.append(OPEN_BRACKET);
    buildPartitionNameValuePair(sb, partitionColumnValMap, partitionTypeMap);
    sb.append(CLOSE_BRACKET);
    sb.append(SPACE);
    sb.append(LOCATION);
    sb.append(SPACE);
    sb.append(SINGLE_QUOTE);
    sb.append(partitionPath);
    sb.append(SINGLE_QUOTE);
    return sb.toString();
  }

  private static String buildSetTablePropertiesQuery(
      String qualifiedTableName,
      String schemaPath
  ) {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format(ALTER_TABLE, qualifiedTableName));
    sb.append(SPACE);
    sb.append(SET);
    sb.append(SPACE);
    sb.append(TBL_PROPERTIES);
    sb.append(SPACE);
    sb.append(OPEN_BRACKET);
    sb.append(buildAvroSchemaTableProperty(schemaPath));
    sb.append(CLOSE_BRACKET);
    return sb.toString();
  }

  private static String buildShowPartitionsQuery(String qualifiedTableName) {
    return String.format(SHOW_PARTITIONS, qualifiedTableName);
  }

  private static String buildDescTableQuery(String qualifiedTableName) {
    return String.format(DESC, qualifiedTableName);
  }

  private static String buildShowTableQuery(String qualifiedTableName) {
    String[] dbTable = qualifiedTableName.split("\\.");
    String db = dbTable[0];
    String table = dbTable[1];
    return String.format(SHOW_TABLES, db, table);
  }

  private void closeStatement(Statement statement) {
    if (statement != null) {
      try {
        statement.close();
      } catch(SQLException e) {
        //It is ok.
        LOG.warn("Error happened when closing statement:", e);
      }
    }
  }

  public boolean executeShowTableQuery(String qualifiedTableName) throws StageException{
    String sql = buildShowTableQuery(qualifiedTableName);
    LOG.debug("Executing SQL:", sql);
    Statement statement = null;
    try (Connection con = DriverManager.getConnection(jdbcUrl)){
      statement = con.createStatement();
      ResultSet rs = statement.executeQuery(sql);
      return rs.next();
    } catch (SQLException e) {
      LOG.error("SQL Exception happened when creating table", e);
      throw new StageException(Errors.HIVE_20, sql, e.getMessage());
    } finally {
      closeStatement(statement);
    }
  }

  public void executeCreateTableQuery(
      String qualifiedTableName,
      LinkedHashMap<String, HiveType> columnTypeMap,
      LinkedHashMap<String, HiveType> partitionTypeMap,
      boolean useAsAvro,
      String schemaLocation,
      boolean isInternal
  ) throws StageException {
    Utils.checkArgument(
        (useAsAvro || schemaLocation != null),
        "Invalid configuration for table creation in use As Avro"
    );

    String sql = useAsAvro? buildCreateTableQueryNew(qualifiedTableName, columnTypeMap, partitionTypeMap, isInternal)
        : buildCreateTableQueryOld(qualifiedTableName, columnTypeMap, partitionTypeMap, schemaLocation, isInternal);

    LOG.debug("Executing SQL:", sql);
    Statement statement = null;
    try (Connection con = DriverManager.getConnection(jdbcUrl)){
      statement = con.createStatement();
      statement.execute(sql);
    } catch (SQLException e) {
      LOG.error("SQL Exception happened when creating table", e);
      throw new StageException(Errors.HIVE_20, sql, e.getMessage());
    } finally {
      closeStatement(statement);
    }
  }

  public void executeAlterTableAddColumnsQuery(
      String qualifiedTableName,
      LinkedHashMap<String, HiveType> columnTypeMap
  ) throws StageException {
    String sql = buildAddColumnsQuery(qualifiedTableName, columnTypeMap);
    LOG.debug("Executing SQL:", sql);
    Statement statement = null;
    try (Connection con = DriverManager.getConnection(jdbcUrl)){
      statement = con.createStatement();
      statement.execute(sql);
    } catch (SQLException e) {
      LOG.error("SQL Exception happened when adding columns", e);
      throw new StageException(Errors.HIVE_20, sql, e.getMessage());
    } finally {
      closeStatement(statement);
    }
  }

  public void executeAlterTableAddPartitionQuery(
      String qualifiedTableName,
      LinkedHashMap<String, String> partitionNameValueMap,
      Map<String, HiveType> partitionTypeMap,
      String partitionPath
  ) throws StageException {
    String sql = buildPartitionAdditionQuery(qualifiedTableName, partitionNameValueMap, partitionTypeMap, partitionPath);
    LOG.debug("Executing SQL:", sql);
    Statement statement = null;
    try (Connection con = DriverManager.getConnection(jdbcUrl)){
      statement = con.createStatement();
      statement.execute(sql);
    } catch (SQLException e) {
      LOG.error("SQL Exception happened when adding partition", e);
      throw new StageException(Errors.HIVE_20, sql, e.getMessage());
    } finally {
      closeStatement(statement);
    }
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
    LOG.debug("Executing SQL:", sql);
    Statement statement = null;
    try (Connection con = DriverManager.getConnection(jdbcUrl)){
      statement = con.createStatement();
      statement.execute(sql);
    } catch (SQLException e) {
      LOG.error("SQL Exception happened when adding partition", e);
      throw new StageException(Errors.HIVE_20, sql, e.getMessage());
    } finally {
      closeStatement(statement);
    }
  }

  /**
   * Returns {@link Set} of partitions
   * @param qualifiedTableName qualified table name
   * @return {@link Set} of partitions
   * @throws StageException in case of any {@link SQLException}
   */
  public Set<LinkedHashMap<String, String>> executeShowPartitionsQuery(String qualifiedTableName) throws StageException {
    String sql = buildShowPartitionsQuery(qualifiedTableName);
    Set<LinkedHashMap<String, String>> partitionInfoSet = new HashSet<>();
    LOG.debug("Executing SQL:", sql);
    Statement statement = null;
    try (Connection con = DriverManager.getConnection(jdbcUrl)){
      statement = con.createStatement();
      ResultSet rs = statement.executeQuery(sql);
      while(rs.next()) {
        String partitionInfoString = rs.getString(1);
        String[] partitionInfoSplit = partitionInfoString.split(HiveMetastoreUtil.SEP);
        LinkedHashMap<String, String> vals = new LinkedHashMap<>();
        for (String partitionValInfo : partitionInfoSplit) {
          String[] partitionNameVal = partitionValInfo.split("=");
          vals.put(partitionNameVal[0], partitionNameVal[1]);
        }
        partitionInfoSet.add(vals);
      }
      return partitionInfoSet;
    } catch (SQLException e) {
      LOG.error("SQL Exception happened when adding partition", e);
      throw new StageException(Errors.HIVE_20, sql, e.getMessage());
    } finally {
      closeStatement(statement);
    }
  }

  private LinkedHashMap<String, HiveType> extractTypeInfo(ResultSet rs) throws SQLException {
    LinkedHashMap<String, HiveType> typeInfo = new LinkedHashMap<>();
    while(rs.next()) {
      String columnName = rs.getString(1);
      if (columnName == null ||columnName.isEmpty()) {
        break;
      }
      String columnType = rs.getString(2);
      typeInfo.put(columnName, HiveType.getHiveTypeFromString(columnType));
    }
    return typeInfo;
  }

  private void processDelimiter(ResultSet rs, String delimiter) throws SQLException {
    if (rs.next()) {
      String columnName = rs.getString(1);
      Utils.checkState(
          (columnName.startsWith(delimiter)),
          "Need to be \"#\" or empty after column information determining Partition Information"
      );
    }
  }

  /**
   * Returns {@link Pair} of Column Type Info and Partition Type Info.
   * @param qualifiedTableName qualified table name.
   * @return {@link Pair} of Column Type Info and Partition Type Info.
   * @throws StageException in case of any {@link SQLException}
   */
  public Pair<LinkedHashMap<String, HiveType>, LinkedHashMap<String, HiveType>> executeDescTableQuery(
      String qualifiedTableName
  ) throws StageException {
    String sql = buildDescTableQuery(qualifiedTableName);
    LOG.debug("Executing SQL:", sql);
    Statement statement = null;
    try (Connection con = DriverManager.getConnection(jdbcUrl)){
      statement = con.createStatement();
      ResultSet rs = statement.executeQuery(sql);
      LinkedHashMap<String, HiveType> columnTypeInfo  = extractTypeInfo(rs);
      processDelimiter(rs, "#");
      processDelimiter(rs, "#");
      processDelimiter(rs, "");
      LinkedHashMap<String, HiveType> partitionTypeInfo = extractTypeInfo(rs);
      //Remove partition columns from the columns map.
      for (String partitionCol : partitionTypeInfo.keySet()) {
        columnTypeInfo.remove(partitionCol);
      }
      return Pair.of(columnTypeInfo, partitionTypeInfo);
    } catch (SQLException e) {
      LOG.error("SQL Exception happened when adding partition", e);
      throw new StageException(Errors.HIVE_20, sql, e.getMessage());
    } finally {
      closeStatement(statement);
    }
  }
}
