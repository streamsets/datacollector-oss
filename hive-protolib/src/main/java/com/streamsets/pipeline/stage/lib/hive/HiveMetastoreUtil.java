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

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.destination.hive.Errors;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;

public final class HiveMetastoreUtil {
  private static final Logger LOG = LoggerFactory.getLogger(HiveMetastoreUtil.class.getCanonicalName());
  private static final String DB_DOT_TABLE = "%s.%s";

  //Common Constants
  private static final String LOCATION_FIELD = "location";
  private static final String TABLE_FIELD = "table";
  private static final String PARTITION_FIELD = "partitions";
  private static final String PARTITION_NAME = "name";
  private static final String COLUMN_NAME = "name";

  //Schema Change Constants
  private static final String COLUMNS_FIELD = "columns";
  private static final String INTERNAL_FIELD = "internal";
  private static final String PARTITION_TYPE = "type";
  private static final String COLUMN_TYPE = "type";

  //Partition Rolling Constants
  private static final String PARTITION_VALUE = "value";

  private static final String AVRO_SCHEMA = "avro_schema";
  private static final String HDFS_SCHEMA_FOLDER_NAME = ".schemas";
  public static final String DATABASE_FIELD = "database";
  public static final String SEP = "/";
  public static final String EQUALS = "=";
  public static final String DEFAULT_DBNAME = "default";


  private HiveMetastoreUtil() {}

  /*
   * Extract information from the list fields of form:
   *
   * Column Type Information : [{name:"column1", type:"string"}, {name:"column2", type:"integer"}]
   * Partition Type Information: [{name:"partition_column1", type:"date"}, {name:"partition_column2", type:"string"}]
   * Partition Value Information : [{name:"column1", value:"07-05-2016"}, {name:"column2", value:"production"}]
   *
   * If any other List field path is given which does not conform  to above
   * form a stage exception with the mentioned error is throws.
   *
   * Returns a linked hash map so as to maintain the order.
   */
  @SuppressWarnings("unchecked")
  private static <T> void extractInnerMapFromTheList(
      Record metadataRecord,
      String listFieldName,
      String innerPairFirstFieldName,
      String innerPairSecondFieldName,
      boolean isSecondFieldHiveType,
      LinkedHashMap<String, T> returnVal,
      StageException exception
  ) throws StageException{
    boolean throwException = false;
    if (metadataRecord.has(SEP + listFieldName)) {
      Field columnField = metadataRecord.get(SEP + listFieldName);
      List<Field> columnList = columnField.getValueAsList();
      if (columnList != null) {
        for (Field listElementField : columnList) {
          if (listElementField.getType() != Field.Type.MAP && listElementField.getType() != Field.Type.LIST_MAP) {
            throwException = true;
            break;
          }
          LinkedHashMap<String, Field> innerPair = listElementField.getValueAsListMap();
          String innerPairFirstField = innerPair.get(innerPairFirstFieldName).getValueAsString();
          String innerPairSecondField = innerPair.get(innerPairSecondFieldName).getValueAsString();
          returnVal.put(
              innerPairFirstField,
              isSecondFieldHiveType? (T)HiveType.getHiveTypeFromString(innerPairSecondField) :(T)innerPairSecondField);
        }
      }
    } else {
      throwException = true;
    }
    if (throwException) {
      throw exception;
    }
  }

  /**
   * Opposite operation of extractInnerMapFromTheList.
   * It takes LinkedHashMap and generate a Field that contains the list.
   * This is to send metadata record to HMS target.
   * This function is called to for partition type list and partition value list.
   */
  private static <T> Field generateInnerFieldFromTheList(
      LinkedHashMap<String, T> original,
      String innerPairFirstFieldName,
      String innerPairSecondFieldName,
      boolean isSecondFieldHiveType
  ) throws StageException {
    List<Field> columnList = new LinkedList<>();

    for(Map.Entry<String,T> pair:  original.entrySet()) {
      Map<String, Field> entry = new LinkedHashMap<>();
      entry.put(innerPairFirstFieldName, Field.create(pair.getKey()));
      if (isSecondFieldHiveType){
        entry.put(innerPairSecondFieldName, Field.create(
            HiveType.getFieldTypeForHiveType((HiveType)pair.getValue()), pair.getValue()));
      } else {
        entry.put(innerPairSecondFieldName, Field.create(pair.getValue().toString())); //stored value is "INT". need to fix this
      }
      columnList.add(Field.create(entry));
    }
    return !columnList.isEmpty() ? Field.create(columnList) : null;
  }

  /**
   * Get qualified table name (defined as dbName.tableName)
   * @param dbName Database name
   * @param tableName Table Name
   * @return the qualified table name.
   */
  public static String getQualifiedTableName(String dbName, String tableName) {
    return (dbName == null || dbName.isEmpty())? tableName : String.format(DB_DOT_TABLE, dbName, tableName);
  }

  /**
   * Extract column information from the column list in "/columns" field.<br>
   *
   * Column Type Information should exist in this form: <br>
   *   [{name:"column1", type:"string"}, {name:"column2", type:"integer"}]
   *
   * @param metadataRecord record which contains the {@link #COLUMNS_FIELD} and conform to the above structure.
   * @return Map of column name to column type
   * @throws StageException if no column information exists or the record has invalid fields.
   */
  public static LinkedHashMap<String, HiveType> getColumnNameType(Record metadataRecord) throws StageException{
    LinkedHashMap<String, HiveType> columnNameType = new LinkedHashMap<>();
    extractInnerMapFromTheList(
        metadataRecord,
        COLUMNS_FIELD,
        COLUMN_NAME,
        COLUMN_TYPE,
        true,
        columnNameType,
        new StageException(Errors.HIVE_17, COLUMNS_FIELD, metadataRecord)
    );
    return columnNameType;
  }

  /**
   * Extract column information from the Partition list in "/partitions" field.<br>
   *
   * Partition Information should exist in this form:<br>
   *   [{name:"column1", type:"date"}, {name:"column2", type:"string"}]
   *
   * @param metadataRecord record which contains the {@link #PARTITION_FIELD} and conform to the above structure.
   * @return Map of partition name to partition type
   * @throws StageException if no partition information exists or the record has invalid fields.
   */
  public static LinkedHashMap<String, HiveType> getPartitionNameType(Record metadataRecord) throws StageException{
    LinkedHashMap<String, HiveType> partitionNameType = new LinkedHashMap<>();
    extractInnerMapFromTheList(
        metadataRecord,
        PARTITION_FIELD,
        PARTITION_NAME,
        PARTITION_TYPE,
        true,
        partitionNameType,
        new StageException(Errors.HIVE_17, PARTITION_FIELD, metadataRecord)
    );
    return partitionNameType;
  }

  /**
   * Extract column information from the Partition list in "/partitions" field.<br>
   *
   * Partition Value Information should exist in this form: <br>
   *   [{name:"column1", value:"07-05-2016"}, {name:"column2", type:"production"}]
   *
   * @param metadataRecord record which contains the {@link #PARTITION_FIELD} and conform to the above structure.
   * @return Map of partition name to partition value
   * @throws StageException if no partition value information exists or the record has invalid fields.
   */
  public static LinkedHashMap<String, String> getPartitionNameValue(Record metadataRecord) throws StageException{
    LinkedHashMap<String, String> partitionNameValue = new LinkedHashMap<>();
    extractInnerMapFromTheList(
        metadataRecord,
        PARTITION_FIELD,
        PARTITION_NAME,
        PARTITION_VALUE,
        false,
        partitionNameValue,
        new StageException(Errors.HIVE_17, PARTITION_FIELD, metadataRecord)
    );
    return partitionNameValue;
  }

  /**
   * Returns true the the metadataRecord has the INTERNAL FIELD (suffice to say this is schema updateState).
   * else return false
   * @param metadataRecord the metadata record
   * @return boolean true or false indicating whether this metadata record is schema change / partition roll record.
   */
  public static boolean isSchemaChangeRecord(Record metadataRecord) {
    return metadataRecord.has(SEP + INTERNAL_FIELD);
  }

  /**
   * Get Table Name from the metadata record.
   * @param metadataRecord the metadata record
   * @return Table Name
   * @throws StageException if the table field does not exist in the metadata record.
   */
  public static String getTableName(Record metadataRecord) throws StageException{
    if (metadataRecord.has(SEP + TABLE_FIELD)) {
      return metadataRecord.get(SEP + TABLE_FIELD).getValueAsString();
    }
    throw new StageException(Errors.HIVE_17, TABLE_FIELD, metadataRecord);
  }

  /**
   * Get Database Name from the metadata record.
   * @param metadataRecord the metadata record
   * @return Database Name
   * @throws StageException if the database name does not exist in the metadata record.
   */
  public static String getDatabaseName(Record metadataRecord) throws StageException{
    if (metadataRecord.has(SEP + DATABASE_FIELD)) {
      String dbName = metadataRecord.get(SEP + DATABASE_FIELD).getValueAsString();
      return dbName.isEmpty()? DEFAULT_DBNAME : dbName;
    }
    throw new StageException(Errors.HIVE_17, DATABASE_FIELD, metadataRecord);
  }

  /**
   * Get internal field from the metadata record.
   * @param metadataRecord the metadata record
   * @return internal field value
   * @throws StageException if the internal field does not exist in the metadata record.
   */
  public static boolean getInternalField(Record metadataRecord) throws StageException{
    if (metadataRecord.has(SEP + INTERNAL_FIELD)) {
      return metadataRecord.get(SEP + INTERNAL_FIELD).getValueAsBoolean();
    }
    throw new StageException(Errors.HIVE_17, INTERNAL_FIELD, metadataRecord);
  }

  /**
   * Get Location from the metadata record.
   * @param metadataRecord the metadata record
   * @return location
   * @throws StageException if the location field does not exist in the metadata record.
   */
  public static String getLocation(Record metadataRecord) throws StageException{
    if (metadataRecord.has(SEP + LOCATION_FIELD)) {
      return metadataRecord.get(SEP + LOCATION_FIELD).getValueAsString();
    }
    throw new StageException(Errors.HIVE_17, LOCATION_FIELD, metadataRecord);
  }

  /**
   * Get Avro Schema from Metadata Record.
   * @param metadataRecord the metadata record.
   * @return Avro Schema
   * @throws StageException if the avro schema field does not exist in the metadata record.
   */
  public static String getAvroSchema(Record metadataRecord) throws StageException{
    if (metadataRecord.has(SEP + AVRO_SCHEMA)) {
      return metadataRecord.get(SEP + AVRO_SCHEMA).getValueAsString();
    }
    throw new StageException(Errors.HIVE_17, AVRO_SCHEMA, metadataRecord);

  }

  /**
   * Fill in metadata to Record. This is for new partition creation.
   */
  public static Field newPartitionMetadataFieldBuilder(
      String database,
      String tableName,
      LinkedHashMap<String, String> partitionList,
      String location) throws StageException {
    LinkedHashMap<String, Field> metadata = new LinkedHashMap<>();
    metadata.put(HiveMetastoreUtil.SEP + HiveMetastoreUtil.DATABASE_FIELD, Field.create(database));
    metadata.put(HiveMetastoreUtil.SEP + HiveMetastoreUtil.TABLE_FIELD, Field.create(tableName));
    metadata.put(HiveMetastoreUtil.SEP + HiveMetastoreUtil.LOCATION_FIELD, Field.create(location));

    //fill in the partition list here
    metadata.put(
        HiveMetastoreUtil.SEP + HiveMetastoreUtil.PARTITION_FIELD,
        generateInnerFieldFromTheList(
            partitionList,
            PARTITION_NAME,
            PARTITION_VALUE,
            false
        )
    );
    return Field.create(metadata);
  }

  /**
   * Fill in metadata to Record. This is for new schema creation.
   */
  public static Field newSchemaMetadataFieldBuilder  (
      String database,
      String tableName,
      LinkedHashMap<String, HiveType> columnList,
      LinkedHashMap<String, HiveType> partitionTypeList,
      boolean internal,
      String location,
      String avroSchema
  ) throws StageException  {
    LinkedHashMap<String, Field> metadata = new LinkedHashMap<>();
    metadata.put(HiveMetastoreUtil.SEP + HiveMetastoreUtil.DATABASE_FIELD, Field.create(database));
    metadata.put(HiveMetastoreUtil.SEP + HiveMetastoreUtil.TABLE_FIELD, Field.create(tableName));
    metadata.put(HiveMetastoreUtil.SEP + HiveMetastoreUtil.LOCATION_FIELD, Field.create(location));

    //fill in column type list here
    metadata.put(
        HiveMetastoreUtil.SEP + HiveMetastoreUtil.COLUMNS_FIELD,
        generateInnerFieldFromTheList(
            columnList,
            COLUMN_NAME,
            COLUMN_TYPE,
            false
        )
    );
    //fill in partition type list here
    metadata.put(
        HiveMetastoreUtil.SEP + HiveMetastoreUtil.PARTITION_FIELD,
        generateInnerFieldFromTheList(
            partitionTypeList,
            PARTITION_NAME,
            PARTITION_TYPE,
            true
        )
    );
    metadata.put(HiveMetastoreUtil.SEP + HiveMetastoreUtil.INTERNAL_FIELD, Field.create(internal));
    metadata.put(HiveMetastoreUtil.SEP + HiveMetastoreUtil.AVRO_SCHEMA, Field.create(avroSchema));
    return Field.create(metadata);
  }

  /**
   * Convert a Record to LinkedHashMap. This is for comparing the structure with TypeInfo in cache.
   * @param record incoming Record
   * @return LinkedHashMap version of record. Key is the column name, and value is column type in HiveType
   * @throws StageException
   */
  public static LinkedHashMap<String, HiveType> convertRecordToHMSType(Record record) throws StageException {
    LinkedHashMap<String, HiveType> columns = new LinkedHashMap<>();
    LinkedHashMap<String, Field> list = record.get().getValueAsListMap();
    for(Map.Entry<String,Field> pair:  list.entrySet()) {
      columns.put(pair.getKey(), HiveType.getHiveTypeforFieldType(pair.getValue().getType()));
    }
    return columns;
  }

  /**
   * Returns the hdfs paths where the avro schema is stored after serializing.
   * Path is appended with current time so as to have an ordering.
   * @param rootTableLocation Root Table Location
   * @return Hdfs Path String.
   */
  public static String serializeSchemaToHDFS(
      UserGroupInformation loginUGI,
      final FileSystem fs,
      final String rootTableLocation,
      final String schemaJson
  ) throws StageException{
    final String folderPath = rootTableLocation + HiveMetastoreUtil.SEP + HiveMetastoreUtil.HDFS_SCHEMA_FOLDER_NAME ;
    final Path schemasFolderPath = new Path(folderPath);
    final String path =  folderPath + SEP + HiveMetastoreUtil.AVRO_SCHEMA + DateFormatUtils.format(
        new Date(System.currentTimeMillis()) ,
        "yyyy-MM-dd--HH_mm_ss"
    );
    try {
      loginUGI.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception{
          if (!fs.exists(schemasFolderPath)) {
            fs.mkdirs(schemasFolderPath);
          }
          Path schemaFilePath = new Path(path);
          //This will never happen unless two HMS targets are writing, we will error out for this
          //and let user handle this via error record handling.
          if (!fs.exists(schemaFilePath)) {
            try (FSDataOutputStream os = fs.create(schemaFilePath)) {
              os.writeChars(schemaJson);
            }
          } else {
            LOG.error(Utils.format("Already schema file {} exists in HDFS", path));
            throw new IOException("Already schema file exists");
          }
          return null;
        }
      });
    } catch (Exception e) {
      LOG.error("Error in Writing Schema to HDFS: " + e.toString(), e);
      throw new StageException(Errors.HIVE_18, path, e.getMessage());
    }
    return path;
  }
}
