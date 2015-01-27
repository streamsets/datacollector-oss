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

  public static String getId() {
    String id = null;
    Record record = (Record) ELEvaluator.getVariablesInScope().getContextVariable(RECORD_CONTEXT_VAR);
    if (record != null) {
      id = record.getHeader().getSourceId();
    }
    return id;
  }

  private static final Method RECORD_TYPE;
  private static final Method RECORD_VALUE;
  private static final Method RECORD_ID;

  static {
    try {
      RECORD_TYPE = ELRecordSupport.class.getMethod("getType", String.class);
      RECORD_VALUE = ELRecordSupport.class.getMethod("getValue", String.class);
      RECORD_ID = ELRecordSupport.class.getMethod("getId");
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void registerRecordFunctions(ELEvaluator elEvaluator) {
    Utils.checkNotNull(elEvaluator, "elEvaluator");
    elEvaluator.registerFunction("record", "type", RECORD_TYPE);
    elEvaluator.registerFunction("record", "value", RECORD_VALUE);
    elEvaluator.registerFunction("record", "id", RECORD_ID);
    for (Field.Type type : Field.Type.values()) {
      elEvaluator.registerConstant(type.toString(), type);
    }
  }

  public static void setRecordInContext(ELEvaluator.Variables variables, Record record) {
    Utils.checkNotNull(variables, "variables");
    variables.addContextVariable(RECORD_CONTEXT_VAR, record);
  }

}
