/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {

  S3_SPOOLDIR_01("Error while processing object '{}' at position '{}': {}"),
  S3_SPOOLDIR_02("Object with key '{}' at offset '{}' exceeds maximum length"),
  S3_SPOOLDIR_03("Object '{}' could not be fully processed, failed on '{}' offset: {}"),
  S3_SPOOLDIR_04("Buffer Limit must be equal or greater than 64KB and equal or less than 1024MB"),
  S3_SPOOLDIR_05("Max files in directory cannot be less than 1"),
  S3_SPOOLDIR_06("File Pattern configuration is required"),

  S3_SPOOLDIR_11("Bucket name cannot be empty"),
  S3_SPOOLDIR_12("Bucket '{}' does not exist"),
  S3_SPOOLDIR_13("Folder name cannot be empty"),
  S3_SPOOLDIR_14("Absolute source folder path cannot be same as the absolute post processing folder path, '{}'"),

  S3_SPOOLDIR_20("Cannot connect to Amazon S3, reason : {}"),
  S3_SPOOLDIR_21("Found invalid offset value '{}'"),
  S3_SPOOLDIR_23("Unable to fetch object, reason : {}"),
  S3_SPOOLDIR_24("Unable to move object, reason : {}"),
  S3_SPOOLDIR_25("Unable to get object content, reason : {}"),
  ;

  private final String msg;
  Errors(String msg) {
    this.msg = msg;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getMessage() {
    return msg;
  }

}
