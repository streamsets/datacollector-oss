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
  S3_SPOOLDIR_04("Object '{}' could not be fully processed, failed on '{}' offset: {}"),
  S3_SPOOLDIR_05("Unsupported charset '{}'"),
  S3_SPOOLDIR_06("Buffer Limit must be equal or greater than 64KB and equal or less than 1024MB"),

  S3_SPOOLDIR_10("Unsupported data format '{}'"),
  S3_SPOOLDIR_11("Bucket name cannot be empty"),
  S3_SPOOLDIR_12("Bucket '{}' does not exist"),
  S3_SPOOLDIR_13("Max files in directory cannot be less than 1"),
  S3_SPOOLDIR_14("Max data object length cannot be less than 1"),

  S3_SPOOLDIR_21("Invalid XML element name '{}'"),
  S3_SPOOLDIR_22("Cannot create the parser factory: {}"),
  S3_SPOOLDIR_23("XML delimiter element cannot be empty"),

  S3_SPOOLDIR_30("Cannot connect to Amazon S3, reason : {}"),
  S3_SPOOLDIR_31("Found invalid offset value '{}'"),
  S3_SPOOLDIR_33("Unable to fetch object, reason : {}"),
  S3_SPOOLDIR_34("Unable to move object, reason : {}"),
  S3_SPOOLDIR_35("Unable to get object content, reason : {}"),
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
