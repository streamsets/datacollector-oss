/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.util;

import com.streamsets.pipeline.api.ErrorCode;

public enum ContainerError implements ErrorCode {
  // Unchecked exception
  CONTAINER_0000("Runtime exception: {}"),

  // StageContext
  CONTAINER_0001("Exception: {}"),
  CONTAINER_0002("Error message: {}"),

  // RequiredFieldsErrorPredicateSink
  CONTAINER_0050("The stage requires records to have the following fields '{}'"),

  // PipelineManager
  CONTAINER_0100("Could not set state, {}"),
  CONTAINER_0101("Could not get state, {}"),
  CONTAINER_0102("Could not change state from {} to {}"),
  CONTAINER_0103("Could not set the source offset during a run"),
  CONTAINER_0104("Could not reset the source offset as the pipeline is running"),
  CONTAINER_0105("Could not capture snapshot because pipeline is not running"),
  CONTAINER_0106("Could not get error records because pipeline is not running"),
  CONTAINER_0107("Invalid batch size supplied {}"),
  CONTAINER_0108("Could not start pipeline manager. Reason : {}"),
  CONTAINER_0109("Pipeline {} does not exist"),

  // PipelineRunners
  CONTAINER_0150("Pipeline configuration error, {}"),
  CONTAINER_0151("Pipeline build error, {}"),
  CONTAINER_0152("Stage '{}', instance '{}', variable '{}', value '{}', configuration injection error: {}"),
  CONTAINER_0153("Stage '{}', instance '{}', missing configuration '{}'"),
  CONTAINER_0154("Cannot preview, {}"),
  CONTAINER_0155("Instance '{}', required fields configuration must be a List, it is a '{}'"),
  CONTAINER_0156("Invalid instance '{}'"),
  CONTAINER_0157("Cannot do a preview stage run on a source, instance '{}'"),
  CONTAINER_0158("Cannot run, {}"),
  CONTAINER_0159("Cannot do a raw source preview as the pipeline '{}' is empty"),
  CONTAINER_0160("Cannot do a raw source preview on source as the following required parameters are not supplied '{}'"),
  CONTAINER_0161("Stage '{}', instance '{}', variable  '{}', configuration injection error, value List has non-String elements"),
  CONTAINER_0162("Stage '{}', instance '{}', variable  '{}', configuration injection error, value Map has non-String keys"),
  CONTAINER_0163("Stage '{}', instance '{}', variable  '{}', configuration injection error, value Map has non-String values"),

  //PipelineStore
  CONTAINER_0200("Pipeline '{}' does not exist"),
  CONTAINER_0201("Pipeline '{}' already exists"),
  CONTAINER_0202("Could not create pipeline '{}', {}"),
  CONTAINER_0203("Could not delete pipeline '{}', {}"),
  CONTAINER_0204("Could not save pipeline '{}', {}"),
  CONTAINER_0205("The provided UUID does not match the stored one, please reload the pipeline '{}'"),
  CONTAINER_0206("Could not load pipeline '{}' info, {}"),

  ;

  private final String msg;

  ContainerError(String msg) {
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
