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
package com.streamsets.pipeline.stage.kinetica.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.Type;
import com.gpudb.protocol.ShowTableResponse;

public class KineticaTableUtils {

  private static final String STRING_TYPE_NAME = "string";
  private static final String LONG_TYPE_NAME = "long";
  private static final String INTEGER_TYPE_NAME = "int";
  private static final String FLOAT_TYPE_NAME = "float";
  private static final String DOUBLE_TYPE_NAME = "double";
  private static final String BYTES_TYPE_NAME = "bytes";

  private final static Logger LOG = LoggerFactory.getLogger(KineticaTableUtils.class);

  ShowTableResponse showTableResponse = null;

  List<String> tableDescription = null;

  private boolean replicated = false;

  private Type type = null;

  private String tableName;

  public KineticaTableUtils(GPUdb gpudb, String tableName) throws GPUdbException {

    this.tableName = tableName;

    showTableResponse = gpudb.showTable(tableName, null);

    tableDescription = getTableDescription();

    validateTableAcceptsInserts();

    type = getTypeForTable();

    replicated = getIsReplicated();

    if (replicated) {
      LOG.info("Table " + tableName + " is replicated");
    } else {
      LOG.info("Table " + tableName + " is not replicated");
    }
  }

  public boolean isReplicated() {
    return replicated;
  }

  public Type getType() {
    return type;
  }

  // Get the table description from the ShowTableResponse
  private List<String> getTableDescription() throws GPUdbException {
    List<List<String>> descriptions = showTableResponse.getTableDescriptions();
    if (descriptions == null || descriptions.size() != 1) {
      throw new GPUdbException("Error getting description for table " + tableName);
    }
    return descriptions.get(0);
  }

  // Make sure table is a valid type to insert data into. We'll throw an Exception
  // if it is not
  private void validateTableAcceptsInserts() throws GPUdbException {
    for (String s : tableDescription) {
      if (s.equalsIgnoreCase("COLLECTION")) {
        throw new GPUdbException("Error: table " + tableName + " is a Collection");
      } else if (s.equalsIgnoreCase("VIEW")) {
        throw new GPUdbException("Error: table " + tableName + " is a View");
      } else if (s.equalsIgnoreCase("JOIN")) {
        throw new GPUdbException("Error: table " + tableName + " is a Join Table");
      } else if (s.equalsIgnoreCase("RESULT_TABLE")) {
        throw new GPUdbException("Error: table " + tableName + " is a Result Table");
      }
    }
  }

  // See if the table is a replicated table
  private boolean getIsReplicated() {
    for (String s : tableDescription) {
      if (s.equalsIgnoreCase("REPLICATED")) {
        return true;
      }
    }
    return false;
  }

  private Type getTypeForTable() throws GPUdbException {

    // This will hold the list of columns we retrieve for the table
    List<Type.Column> columns = new ArrayList<Type.Column>();

    // Get JSON schema for table
    JSONObject schema = getTableSchema(tableName, showTableResponse);

    // Get extended column properties
    Map<String, List<String>> columnPropeties = getColumnProperties(tableName, showTableResponse);

    // Get the fields from the schema
    JSONArray fields = schema.getJSONArray("fields");

    // For each field, get its name, type and extended properties
    for (int i = 0; i < fields.length(); i++) {
      JSONObject field = fields.getJSONObject(i);

      // Get the column's name
      String columnName = field.getString("name");

      // Get the column's type
      Class<?> columnType = getColumnType(field);

      // Get the column's extended properties
      List<String> columnProps = columnPropeties.get(columnName);
      if (!columnProps.contains("nullable") && typeIsNullable(field)) {
        columnProps.add("nullable");
      }

      // Create the Type.Column
      Type.Column column = new Type.Column(columnName, columnType, columnProps);

      // Add the Type.Column to the list of Type.Columns
      columns.add(column);

      LOG.debug("Adding column name: " + columnName + " type: " + columnType.toString() + " props: "
          + columnProps.toString());

    }
    return new Type(columns);
  }

  // Get the Class for the column type
  private Class<?> getColumnType(JSONObject field) throws GPUdbException {

    Class<?> columnType = null;

    // The Avro "type" element might be an array if the type is nullable
    if (field.get("type") instanceof JSONArray) {
      JSONArray columnTypes = field.getJSONArray("type");
      for (int j = 0; j < columnTypes.length(); j++) {
        String ct = (String) columnTypes.get(j);
        if (!ct.equals("null")) {
          columnType = getClassForType(ct);
          break;
        }
      }
    } else {
      columnType = getClassForType(field.getString("type"));
    }
    if (columnType == null) {
      throw new GPUdbException("Error getting column type for field: " + field.toString());
    }
    return columnType;
  }

  // See if the type is nullable (it will be set as "null" in the JSON array in
  // the Avro "type" field)
  private boolean typeIsNullable(JSONObject field) throws GPUdbException {
    if (field.get("type") instanceof JSONArray) {
      JSONArray columnTypes = field.getJSONArray("type");
      for (int j = 0; j < columnTypes.length(); j++) {
        String ct = (String) columnTypes.get(j);
        if (ct.equals("null")) {
          return true;
        }
      }
    }
    return false;
  }

  // Get the table's schema as a JSON Object
  private JSONObject getTableSchema(String tableName, ShowTableResponse showTableResponse) throws GPUdbException {

    List<String> schemas = showTableResponse.getTypeSchemas();

    if (schemas == null || schemas.size() != 1) {
      throw new GPUdbException("Error getting schema for table " + tableName);
    }

    return new JSONObject(schemas.get(0));
  }

  // Get the table's extended column properties
  private Map<String, List<String>> getColumnProperties(String tableName, ShowTableResponse showTableResponse)
      throws GPUdbException {

    List<Map<String, List<String>>> columnPropertiesList = showTableResponse.getProperties();

    if (columnPropertiesList == null || columnPropertiesList.size() != 1) {
      throw new GPUdbException("Error getting properties for table " + tableName);
    }

    return columnPropertiesList.get(0);
  }

  // Get the Java type for a type name
  private Class<?> getClassForType(String typeName) throws GPUdbException {

    typeName = typeName.replace(" ", "");
    if (typeName.equalsIgnoreCase(STRING_TYPE_NAME)) {
      return String.class;
    } else if (typeName.equalsIgnoreCase(LONG_TYPE_NAME)) {
      return Long.class;
    } else if (typeName.equalsIgnoreCase(INTEGER_TYPE_NAME)) {
      return Integer.class;
    } else if (typeName.equalsIgnoreCase(FLOAT_TYPE_NAME)) {
      return Float.class;
    } else if (typeName.equalsIgnoreCase(DOUBLE_TYPE_NAME)) {
      return Double.class;
    } else if (typeName.equalsIgnoreCase(BYTES_TYPE_NAME)) {
      return ByteBuffer.class;
    } else {
      throw new GPUdbException("Error: unknown type '" + typeName + "' in table schema");
    }
  }

}