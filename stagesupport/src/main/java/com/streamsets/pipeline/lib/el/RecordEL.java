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
package com.streamsets.pipeline.lib.el;

import com.streamsets.pipeline.api.ElConstant;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

//IMPORTANT: make sure to keep in sync defined EL functions with FakeRecordEL
public class RecordEL {

  public static final String RECORD_EL_PREFIX = "record";

  private static final String RECORD_CONTEXT_VAR = "record";

  public static Record getRecordInContext() {
    return (Record) ELEval.getVariablesInScope().getContextVariable(RECORD_CONTEXT_VAR);
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "type",
      description = "Returns the type of the field represented by path 'fieldPath' for the record in context")
  public static Field.Type getType(
      @ElParam("fieldPath") String fieldPath) {
    Field.Type type = null;
    Record record = getRecordInContext();
    if (record != null) {
      Field field = record.get(fieldPath);
      if (field != null) {
        type = field.getType();
      }
    }
    return type;
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "value",
      description = "Returns the value of the field represented by path 'fieldPath' for the record in context")
  @SuppressWarnings("unchecked")
  public static Object getValue(
      @ElParam("fieldPath") String fieldPath) {
    Object value = null;
    Record record = getRecordInContext();
    if (record != null) {
      Field field = record.get(fieldPath);
      if (field != null) {
        value = field.getValue();
      }
    }
    return value;
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "valueOrDefault",
      description = "Returns the value of the field represented by path 'fieldPath' for the record in context or "
          + "the default value if the field is not present or if the field is null")
  public static Object getValueOrDefault(
      @ElParam("fieldPath") String fieldPath, @ElParam("defaultValue") Object defaultValue) {
    Object value = null;
    Record record = getRecordInContext();
    if (record != null) {
      Field field = record.get(fieldPath);
      if (field != null && field.getValue() != null) {
        value = field.getValue();
      } else {
        value = defaultValue;
      }
    }
    return value;
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "attributeOrDefault",
      description = "Returns the value of the attribute represented by 'attributeName' for the record in context or "
          + "the default value if the attribute is not present or if the attribute is null")
  public static Object getAttributeOrDefault(
      @ElParam("attributeName") String attributeName, @ElParam("defaultValue") String defaultValue) {
    Record record = getRecordInContext();
    if (record != null) {
      Object attributeValue = record.getHeader().getAttribute(attributeName);
      if (attributeValue != null) {
        return attributeValue;
      }
    }
    return defaultValue;
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "exists",
      description = "Checks if the field represented by path 'fieldPath' exists in the record")
  public static boolean exists(
      @ElParam("fieldPath") String fieldPath) {
    Record record = getRecordInContext();
    if (record != null) {
      return record.has(fieldPath);
    }
    return false;
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "fieldAttribute",
      description = "Returns the value of the attribute named 'attributeName' of the field specified by 'fieldPath'")
  @SuppressWarnings("unchecked")
  public static String getFieldAttributeValue(
      @ElParam("fieldPath") String fieldPath, @ElParam("attributeName") String attributeName) {
    Record record = getRecordInContext();
    if (record != null) {
      Field field = record.get(fieldPath);
      if (field != null) {
        return field.getAttribute(attributeName);
      }
    }
    return null;
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "fieldAttributeOrDefault",
      description = "Returns the value of the attribute named 'attributeName' of the field specified by 'fieldPath'" +
          " or the 'defaultValue' if not found "
  )
  @SuppressWarnings("unchecked")
  public static String getFieldAttributeValueOrDefault(
      @ElParam("fieldPath") String fieldPath,
      @ElParam("attributeName") String attributeName,
      @ElParam("defaultValue") String defaultValue
  ) {
    String value = getFieldAttributeValue(fieldPath, attributeName);
    return value != null ? value : defaultValue;
  }

  private enum HeaderProperty {
    ID,
    STAGE_CREATOR,
    STAGES_PATH,
    ERROR_STAGE,
    ERROR_STAGE_LABEL,
    ERROR_CODE,
    ERROR_MESSAGE,
    ERROR_STACK_TRACE,
    ERROR_DATA_COLLECTOR_ID,
    ERROR_PIPELINE_NAME,
    ERROR_JOB_ID,
    ERROR_JOB_NAME,
    ERROR_TIME,
    EVENT_TYPE,
    EVENT_VERSION,
    EVENT_CREATION,
  }

  @SuppressWarnings("unchecked")
  private static <T> T getFromHeader(HeaderProperty prop) {
    Object value = null;
    Record record = getRecordInContext();
    if (record != null) {
      switch (prop) {
        case ID:
          value = record.getHeader().getSourceId();
          break;
        case STAGE_CREATOR:
          value = record.getHeader().getStageCreator();
          break;
        case STAGES_PATH:
          value = record.getHeader().getStagesPath();
          break;
        case ERROR_STAGE:
          value = record.getHeader().getErrorStage();
          break;
        case ERROR_STAGE_LABEL:
          value = record.getHeader().getErrorStageLabel();
          break;
        case ERROR_CODE:
          value = record.getHeader().getErrorCode();
          break;
        case ERROR_MESSAGE:
          value = record.getHeader().getErrorMessage();
          break;
        case ERROR_STACK_TRACE:
          value = record.getHeader().getErrorStackTrace();
          break;
        case ERROR_DATA_COLLECTOR_ID:
          value = record.getHeader().getErrorDataCollectorId();
          break;
        case ERROR_PIPELINE_NAME:
          value = record.getHeader().getErrorPipelineName();
          break;
        case ERROR_JOB_ID:
          value = record.getHeader().getErrorJobId();
          break;
        case ERROR_JOB_NAME:
          value = record.getHeader().getErrorJobName();
          break;
        case ERROR_TIME:
          value = record.getHeader().getErrorTimestamp();
          break;
        case EVENT_TYPE:
          value = record.getHeader().getAttribute(EventRecord.TYPE);
          break;
        case EVENT_VERSION:
          value = record.getHeader().getAttribute(EventRecord.VERSION);
          break;
        case EVENT_CREATION:
          value = record.getHeader().getAttribute(EventRecord.CREATION_TIMESTAMP);
          break;
      }
    }
    return (T) value;
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "id",
      description = "Returns the id of the record in context")
  public static String getId() {
    return getFromHeader(HeaderProperty.ID);
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "creator",
      description = "Returns the id of the record in context")
  public static String getStageCreator() {
    return getFromHeader(HeaderProperty.STAGE_CREATOR);
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "path",
      description = "Returns the stage path for the record in context")
  public static String getStagesPath() {
    return getFromHeader(HeaderProperty.STAGES_PATH);
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "errorStage",
      description = "Returns the error stage for the record in context")
  public static String getErrorStage() {
    return getFromHeader(HeaderProperty.ERROR_STAGE);
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "errorStageLabel",
      description = "Returns the error stage label for the record in context")
  public static String getErrorStageLabel() {
    return getFromHeader(HeaderProperty.ERROR_STAGE_LABEL);
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "errorCode",
      description = "Returns the error code for the record in context")
  public static String getErrorCode() {
    return getFromHeader(HeaderProperty.ERROR_CODE);
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "errorMessage",
      description = "Returns the error message for the record in context")
  public static String getErrorMessage() {
    return getFromHeader(HeaderProperty.ERROR_MESSAGE);
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "errorStackTrace",
      description = "Returns the error stack trace for the record in context")
  public static String getErrorStackTrace() {
    return getFromHeader(HeaderProperty.ERROR_STACK_TRACE);
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "errorCollectorId",
      description = "Returns the error data collector id for the record in context")
  public static String getErrorDataCollectorId() {
    return getFromHeader(HeaderProperty.ERROR_DATA_COLLECTOR_ID);
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "errorPipeline",
      description = "Returns the error pipeline name for the record in context")
  public static String getErrorPipelineName() {
    return getFromHeader(HeaderProperty.ERROR_PIPELINE_NAME);
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "errorJobId",
      description = "Returns the error job id for the record in context")
  public static String getErrorJobId() {
    return getFromHeader(HeaderProperty.ERROR_JOB_ID);
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "errorJobName",
      description = "Returns the error job name for the record in context")
  public static String getErrorJobName() {
    return getFromHeader(HeaderProperty.ERROR_JOB_NAME);
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "errorTime",
      description = "Returns the error time for the record in context")
  public static long getErrorTime() {
    return getFromHeader(HeaderProperty.ERROR_TIME);
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "eventType",
      description = "Returns type of the event for event records and null for non-event records.")
  public static String getEventType() {
    return getFromHeader(HeaderProperty.EVENT_TYPE);
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "eventVersion",
      description = "Returns version of the event for event records and null for non-event records.")
  public static String getEventVersion() {
    return getFromHeader(HeaderProperty.EVENT_VERSION);
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "eventCreation",
      description = "Returns creation time of the event for event records and null for non-event records.")
  public static String getEventCreationTime() {
    return getFromHeader(HeaderProperty.EVENT_CREATION);
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "attribute",
      description = "Returns a record header attribute")
  public static String getAttribute(
      @ElParam("name") String name
  ) {
    String attribute = null;
    Record record = getRecordInContext();
    if (record != null) {
      attribute = record.getHeader().getAttribute(name);
    }
    return attribute;
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "dValue",
      description = "Returns the value of the specified header name")
  public static String getDelimitedValue(
      @ElParam("header") String header) {
    String value = null;
    Record record = getRecordInContext();
    if (record != null) {
      Field root = record.get();
      if (root != null && root.getType() == Field.Type.LIST && root.getValue() != null) {
        List<Field> list = root.getValueAsList();
        for (Field element : list) {
          if (element.getType() == Field.Type.MAP && element.getValue() != null) {
            Map<String, Field> map = element.getValueAsMap();
            if (map.containsKey("header")) {
              Field headerField = map.get("header");
              if (headerField.getType() == Field.Type.STRING && headerField.getValue() != null) {
                if (headerField.getValueAsString().equals(header)) {
                  if (map.containsKey("value")) {
                    Field valueField = map.get("value");
                    if (valueField.getType() == Field.Type.STRING && valueField.getValue() != null) {
                      value = valueField.getValueAsString();
                      break;
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    return value;
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "dExists",
      description = "Returns the value of the specified header name")
  public static boolean getDelimitedExists(
      @ElParam("header") String header) {
    return getDelimitedValue(header) != null;
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "dValueAt",
      description = "Returns the value of the specified header name")
  public static String getDelimitedValueAt(
      @ElParam("index") int index) {
    String value = null;
    if (index >= 0) {
      Record record = getRecordInContext();
      if (record != null) {
        Field root = record.get();
        if (root != null && root.getType() == Field.Type.LIST && root.getValue() != null) {
          List<Field> list = root.getValueAsList();
          if (index < list.size()) {
            Field element = list.get(index);
            if (element.getType() == Field.Type.MAP && element.getValue() != null) {
              Map<String, Field> map = element.getValueAsMap();
              if (map.containsKey("value")) {
                Field valueField = map.get("value");
                if (valueField.getType() == Field.Type.STRING && valueField.getValue() != null) {
                  value = valueField.getValueAsString();
                }
              }
            }
          }
        }
      }
    }
    return value;
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "dToMap",
      description = "Converts Columns to a Map")
  public static Map getDelimitedAsMap() {
    Map<String, Field> asMap = new LinkedHashMap<>();
    Record record = getRecordInContext();
    if (record != null) {
      Field root = record.get();
      if (root != null && root.getType() == Field.Type.LIST && root.getValue() != null) {
        List<Field> list = root.getValueAsList();
        for (int i = 0; i < list.size(); i++) {
          Field element = list.get(i);
          if (element.getType() == Field.Type.MAP && element.getValue() != null) {
            Map<String, Field> map = element.getValueAsMap();
            Field nameField = map.get("header");
            String name;
            if (nameField != null && nameField.getType() == Field.Type.STRING && nameField.getValue() != null) {
              name = nameField.getValueAsString();
            } else {
              name = Integer.toString(i);
            }
            asMap.put(name, map.get("value"));
          }
        }
      }
    }
    return asMap;
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "dIndex",
      description = "Returns the index of the specific header name")
  public static int getDelimitedIndex(
      @ElParam("header") String header
  ) {
    return getDelimitedIndex(header, 0);
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "dIsDupHeader",
      description = "Returns if a header is more than once")
  public static boolean isDelimitedDuplicateHeader(
      @ElParam("header") String header
  ) {
    boolean dup = false;
    int idx = getDelimitedIndex(header, 0);
    if (idx > -1) {
      dup = getDelimitedIndex(header, idx + 1) > 0;
    }
    return dup;
  }

  @ElFunction(
      prefix = RECORD_EL_PREFIX,
      name = "dHasDupHeaders",
      description = "Returns if there are duplicate headers")
  public static boolean hasDelimitedDuplicateHeaders() {
    boolean dup = false;
    Record record = getRecordInContext();
    if (record != null) {
      Field root = record.get();
      if (root != null && root.getType() == Field.Type.LIST && root.getValue() != null) {
        Set<String> headers = new HashSet<>();
        List<Field> list = root.getValueAsList();
        for (Field element : list) {
          if (element.getType() == Field.Type.MAP && element.getValue() != null) {
            Map<String, Field> map = element.getValueAsMap();
            Field nameField = map.get("header");
            String name;
            if (nameField != null && nameField.getType() == Field.Type.STRING && nameField.getValue() != null) {
              name = nameField.getValueAsString();
              if (headers.contains(name)) {
                dup = true;
                break;
              } else {
                headers.add(name);
              }
            }
          }
        }
      }
    }
    return dup;
  }

  private static int getDelimitedIndex(String header, int startIndex) {
    int index = -1;
    Record record = getRecordInContext();
    if (record != null) {
      Field root = record.get();
      if (root != null && root.getType() == Field.Type.LIST && root.getValue() != null) {
        List<Field> list = root.getValueAsList();
        for (int i= startIndex; index == -1 && i < list.size(); i++) {
          Field element = list.get(i);
          if (element.getType() == Field.Type.MAP && element.getValue() != null) {
            Map<String, Field> map = element.getValueAsMap();
            if (map.containsKey("header")) {
              Field headerField = map.get("header");
              if (headerField.getType() == Field.Type.STRING && headerField.getValue() != null) {
                if (headerField.getValueAsString().equals(header)) {
                  index = i;
                }
              }
            }
          }
        }
      }
    }
    return index;
  }

  //Declare field types as constants
  @ElConstant(name = "NUMBER", description = "Field Type Integer")
  public static Field.Type INTEGER = Field.Type.INTEGER;

  @ElConstant(name = "BOOLEAN", description = "Field Type Boolean")
  public static Field.Type BOOLEAN = Field.Type.BOOLEAN;

  @ElConstant(name = "BYTE", description = "Field Type Byte")
  public static Field.Type BYTE = Field.Type.BYTE;

  @ElConstant(name = "BYTE_ARRAY", description = "Field Type Byte Array")
  public static Field.Type BYTE_ARRAY = Field.Type.BYTE_ARRAY;

  @ElConstant(name = "CHAR", description = "Field Type Char")
  public static Field.Type CHAR = Field.Type.CHAR;

  @ElConstant(name = "DATE", description = "Field Type Date")
  public static Field.Type DATE = Field.Type.DATE;

  @ElConstant(name = "TIME", description = "Field Type Time")
  public static Field.Type TIME = Field.Type.TIME;

  @ElConstant(name = "DATETIME", description = "Field Type Date Time")
  public static Field.Type DATETIME = Field.Type.DATETIME;

  @ElConstant(name = "DECIMAL", description = "Field Type Decimal")
  public static Field.Type DECIMAL = Field.Type.DECIMAL;

  @ElConstant(name = "DOUBLE", description = "Field Type Double")
  public static Field.Type DOUBLE = Field.Type.DOUBLE;

  @ElConstant(name = "FLOAT", description = "Field Type Float")
  public static Field.Type FLOAT = Field.Type.FLOAT;

  @ElConstant(name = "LIST", description = "Field Type List")
  public static Field.Type LIST = Field.Type.LIST;

  @ElConstant(name = "MAP", description = "Field Type Map")
  public static Field.Type MAP = Field.Type.MAP;

  @ElConstant(name = "LIST_MAP", description = "Field Type List-Map")
  public static Field.Type LIST_MAP = Field.Type.LIST_MAP;

  @ElConstant(name = "LONG", description = "Field Type Long")
  public static Field.Type LONG = Field.Type.LONG;

  @ElConstant(name = "SHORT", description = "Field Type Short")
  public static Field.Type SHORT = Field.Type.SHORT;

  @ElConstant(name = "STRING", description = "Field Type String")
  public static Field.Type STRING = Field.Type.STRING;


  public static void setRecordInContext(ELVars variables, Record record) {
    Utils.checkNotNull(variables, "variables");
    variables.addContextVariable(RECORD_CONTEXT_VAR, record);
  }
}
