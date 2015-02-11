/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;

import java.lang.reflect.Method;

public class ELRecordSupport {

  private static final String RECORD_CONTEXT_VAR = "record";

  public static Field.Type getType(String fieldPath) {
    Field.Type type = null;
    Record record = (Record) ELEvaluator.getVariablesInScope().getContextVariable(RECORD_CONTEXT_VAR);
    if (record != null) {
      Field field = record.get(fieldPath);
      if (field != null) {
        type = field.getType();
      }
    }
    return type;
  }

  public static Object getValue(String fieldPath) {
    Object value = null;
    Record record = (Record) ELEvaluator.getVariablesInScope().getContextVariable(RECORD_CONTEXT_VAR);
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
    Record record = (Record) ELEvaluator.getVariablesInScope().getContextVariable(RECORD_CONTEXT_VAR);
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

  public static String getId() {
    return getFromHeader(HeaderProperty.ID);
  }

  public static String getStageCreator() {
    return getFromHeader(HeaderProperty.STAGE_CREATOR);
  }

  public static String getStagesPath() {
    return getFromHeader(HeaderProperty.STAGES_PATH);
  }

  public static String getErrorStage() {
    return getFromHeader(HeaderProperty.ERROR_STAGE);
  }

  public static String getErrorCode() {
    return getFromHeader(HeaderProperty.ERROR_CODE);
  }

  public static String getErrorMessage() {
    return getFromHeader(HeaderProperty.ERROR_MESSAGE);
  }

  public static String getErrorDataCollectorId() {
    return getFromHeader(HeaderProperty.ERROR_DATA_COLLECTOR_ID);
  }

  public static String getErrorPipelineName() {
    return getFromHeader(HeaderProperty.ERROR_PIPELINE_NAME);
  }

  public static long getErrorTime() {
    return getFromHeader(HeaderProperty.ERROR_TIME);
  }

  private static final Method RECORD_TYPE;
  private static final Method RECORD_VALUE;
  private static final Method RECORD_ID;
  private static final Method RECORD_STAGE_CREATOR;
  private static final Method RECORD_STAGES_PATH;
  private static final Method ERROR_STAGE;
  private static final Method ERROR_CODE;
  private static final Method ERROR_MESSAGE;
  private static final Method ERROR_DATA_COLLECTOR_ID;
  private static final Method ERROR_PIPELINE_NAME;
  private static final Method ERROR_TIME;

  static {
    try {
      RECORD_TYPE = ELRecordSupport.class.getMethod("getType", String.class);
      RECORD_VALUE = ELRecordSupport.class.getMethod("getValue", String.class);
      RECORD_ID = ELRecordSupport.class.getMethod("getId");
      RECORD_STAGE_CREATOR = ELRecordSupport.class.getMethod("getStageCreator");
      RECORD_STAGES_PATH = ELRecordSupport.class.getMethod("getStagesPath");
      ERROR_STAGE = ELRecordSupport.class.getMethod("getErrorStage");
      ERROR_CODE = ELRecordSupport.class.getMethod("getErrorCode");
      ERROR_MESSAGE = ELRecordSupport.class.getMethod("getErrorMessage");
      ERROR_DATA_COLLECTOR_ID = ELRecordSupport.class.getMethod("getErrorDataCollectorId");
      ERROR_PIPELINE_NAME = ELRecordSupport.class.getMethod("getErrorPipelineName");
      ERROR_TIME = ELRecordSupport.class.getMethod("getErrorTime");
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void registerRecordFunctions(ELEvaluator elEvaluator) {
    Utils.checkNotNull(elEvaluator, "elEvaluator");
    elEvaluator.registerFunction("record", "type", RECORD_TYPE);
    elEvaluator.registerFunction("record", "value", RECORD_VALUE);
    elEvaluator.registerFunction("record", "id", RECORD_ID);
    elEvaluator.registerFunction("record", "creator", RECORD_STAGE_CREATOR);
    elEvaluator.registerFunction("record", "path", RECORD_STAGES_PATH);
    elEvaluator.registerFunction("error", "stage", ERROR_STAGE);
    elEvaluator.registerFunction("error", "code", ERROR_CODE);
    elEvaluator.registerFunction("error", "message", ERROR_MESSAGE);
    elEvaluator.registerFunction("error", "collectorId", ERROR_DATA_COLLECTOR_ID);
    elEvaluator.registerFunction("error", "pipeline", ERROR_PIPELINE_NAME);
    elEvaluator.registerFunction("error", "time", ERROR_TIME);
    for (Field.Type type : Field.Type.values()) {
      elEvaluator.registerConstant(type.toString(), type);
    }
  }

  public static void setRecordInContext(ELEvaluator.Variables variables, Record record) {
    Utils.checkNotNull(variables, "variables");
    variables.addContextVariable(RECORD_CONTEXT_VAR, record);
  }

}
