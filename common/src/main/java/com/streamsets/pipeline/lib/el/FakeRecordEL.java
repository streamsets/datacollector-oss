/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.el;

import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;

//Mimics all record: EL functions returning '*'
public class FakeRecordEL {

  public static final String RECORD_EL_PREFIX = "record";

  @ElFunction(
    prefix = RECORD_EL_PREFIX,
    name = "type",
    description = "Returns the type of the field represented by path 'fieldPath' for the record in context")
  public static String getType(
    @ElParam("fieldPath") String fieldPath) {
    return "*";
  }

  @ElFunction(
    prefix = RECORD_EL_PREFIX,
    name = "value",
    description = "Returns the value of the field represented by path 'fieldPath' for the record in context")
  public static String getValue(
    @ElParam("fieldPath") String fieldPath) {
    return "*";
  }

  @ElFunction(
    prefix = RECORD_EL_PREFIX,
    name = "exists",
    description = "Checks if the field represented by path 'fieldPath' exists in the record")
  public static String exists(
    @ElParam("fieldPath") String fieldPath) {
    return "*";
  }

  @ElFunction(
    prefix = RECORD_EL_PREFIX,
    name = "id",
    description = "Returns the id of the record in context")
  public static String getId() {
    return "*";
  }

  @ElFunction(
    prefix = RECORD_EL_PREFIX,
    name = "creator",
    description = "Returns the id of the record in context")
  public static String getStageCreator() {
    return "*";
  }

  @ElFunction(
    prefix = RECORD_EL_PREFIX,
    name = "path",
    description = "Returns the stage path for the record in context")
  public static String getStagesPath() {
    return "*";
  }

  @ElFunction(
    prefix = RECORD_EL_PREFIX,
    name = "errorStage",
    description = "Returns the error stage for the record in context")
  public static String getErrorStage() {
    return "*";
  }

  @ElFunction(
    prefix = RECORD_EL_PREFIX,
    name = "errorCode",
    description = "Returns the error code for the record in context")
  public static String getErrorCode() {
    return "*";
  }

  @ElFunction(
    prefix = RECORD_EL_PREFIX,
    name = "errorMessage",
    description = "Returns the error message for the record in context")
  public static String getErrorMessage() {
    return "*";
  }

  @ElFunction(
    prefix = RECORD_EL_PREFIX,
    name = "errorCollectorId",
    description = "Returns the error data collector id for the record in context")
  public static String getErrorDataCollectorId() {
    return "*";
  }

  @ElFunction(
    prefix = RECORD_EL_PREFIX,
    name = "errorPipeline",
    description = "Returns the error pipeline name for the record in context")
  public static String getErrorPipelineName() {
    return "*";
  }

  @ElFunction(
    prefix = RECORD_EL_PREFIX,
    name = "errorTime",
    description = "Returns the error time for the record in context")
  public static String getErrorTime() {
    return "*";
  }

}
