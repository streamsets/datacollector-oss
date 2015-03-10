/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.el;

import com.streamsets.pipeline.api.ElConstant;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.impl.Utils;

public class RecordEL {

  private static final String RECORD_CONTEXT_VAR = "record";
  private static final String ERROR_CONTEXT_VAR = "error";

  @ElFunction(
    prefix = RECORD_CONTEXT_VAR,
    name = "type",
    description = "Returns the type of the field represented by path 'fieldPath' for the record in context")
  public static Field.Type getType(
    @ElParam("fieldPath") String fieldPath) {
    Field.Type type = null;
    Record record = (Record) ELEval.getVariablesInScope().getContextVariable(RECORD_CONTEXT_VAR);
    if (record != null) {
      Field field = record.get(fieldPath);
      if (field != null) {
        type = field.getType();
      }
    }
    return type;
  }

  @ElFunction(
    prefix = RECORD_CONTEXT_VAR,
    name = "value",
    description = "Returns the value of the field represented by path 'fieldPath' for the record in context")
  public static Object getValue(
    @ElParam("fieldPath") String fieldPath) {
    Object value = null;
    Record record = (Record) ELEval.getVariablesInScope().getContextVariable(RECORD_CONTEXT_VAR);
    if (record != null) {
      Field field = record.get(fieldPath);
      if (field != null) {
        value = field.getValue();
      }
    }
    return value;
  }

  private enum HeaderProperty {
    ID, STAGE_CREATOR, STAGES_PATH, ERROR_STAGE, ERROR_CODE, ERROR_MESSAGE, ERROR_DATA_COLLECTOR_ID,
    ERROR_PIPELINE_NAME, ERROR_TIME
  }

  private static <T> T getFromHeader(HeaderProperty prop) {
    Object value = null;
    Record record = (Record) ELEval.getVariablesInScope().getContextVariable(RECORD_CONTEXT_VAR);
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
        case ERROR_CODE:
          value = record.getHeader().getErrorCode();
          break;
        case ERROR_MESSAGE:
          value = record.getHeader().getErrorMessage();
          break;
        case ERROR_DATA_COLLECTOR_ID:
          value = record.getHeader().getErrorDataCollectorId();
          break;
        case ERROR_PIPELINE_NAME:
          value = record.getHeader().getErrorPipelineName();
          break;
        case ERROR_TIME:
          value = record.getHeader().getErrorTimestamp();
          break;
      }
    }
    return (T) value;
  }

  @ElFunction(
    prefix = RECORD_CONTEXT_VAR,
    name = "id",
    description = "Returns the id of the record in context")
  public static String getId() {
    return getFromHeader(HeaderProperty.ID);
  }

  @ElFunction(
    prefix = RECORD_CONTEXT_VAR,
    name = "creator",
    description = "Returns the id of the record in context")
  public static String getStageCreator() {
    return getFromHeader(HeaderProperty.STAGE_CREATOR);
  }

  @ElFunction(
    prefix = RECORD_CONTEXT_VAR,
    name = "path",
    description = "Returns the stage path for the record in context")
  public static String getStagesPath() {
    return getFromHeader(HeaderProperty.STAGES_PATH);
  }

  @ElFunction(
    prefix = ERROR_CONTEXT_VAR,
    name = "stage",
    description = "Returns the error stage for the record in context")
  public static String getErrorStage() {
    return getFromHeader(HeaderProperty.ERROR_STAGE);
  }

  @ElFunction(
    prefix = ERROR_CONTEXT_VAR,
    name = "code",
    description = "Returns the error code for the record in context")
  public static String getErrorCode() {
    return getFromHeader(HeaderProperty.ERROR_CODE);
  }

  @ElFunction(
    prefix = ERROR_CONTEXT_VAR,
    name = "message",
    description = "Returns the error message for the record in context")
  public static String getErrorMessage() {
    return getFromHeader(HeaderProperty.ERROR_MESSAGE);
  }

  @ElFunction(
    prefix = ERROR_CONTEXT_VAR,
    name = "collectorId",
    description = "Returns the error data collector id for the record in context")
  public static String getErrorDataCollectorId() {
    return getFromHeader(HeaderProperty.ERROR_DATA_COLLECTOR_ID);
  }

  @ElFunction(
    prefix = ERROR_CONTEXT_VAR,
    name = "pipeline",
    description = "Returns the error pipeline name for the record in context")
  public static String getErrorPipelineName() {
    return getFromHeader(HeaderProperty.ERROR_PIPELINE_NAME);
  }

  @ElFunction(
    prefix = ERROR_CONTEXT_VAR,
    name = "time",
    description = "Returns the error time for the record in context")
  public static long getErrorTime() {
    return getFromHeader(HeaderProperty.ERROR_TIME);
  }

  //Declare field types as constants
  @ElConstant(name = "INTEGER", description = "Field Type Integer")
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

  @ElConstant(name = "LONG", description = "Field Type Long")
  public static Field.Type LONG = Field.Type.LONG;

  @ElConstant(name = "SHORT", description = "Field Type Short")
  public static Field.Type SHORT = Field.Type.SHORT;

  @ElConstant(name = "STRING", description = "Field Type String")
  public static Field.Type STRING = Field.Type.STRING;


  public static void setRecordInContext(ELEval.Variables variables, Record record) {
    Utils.checkNotNull(variables, "variables");
    variables.addContextVariable(RECORD_CONTEXT_VAR, record);
  }
}
