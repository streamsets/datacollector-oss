/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.jms;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum JmsErrors implements ErrorCode {
  // Configuration errors
  JMS_00("Could not create initial context '{}' with provider URL '{}' : {}"),
  JMS_01("Could not create connection factory '{}' : {}"),
  JMS_02("Unable to create connection using '{}': {}"),
  JMS_03("Unable to create connection using '{}' with credentials '{}': {}"),
  JMS_04("Unable to start connection: {}"),
  JMS_05("Unable to find destination '{}': {}"),
  JMS_06("Unable to create session: {}"),
  JMS_07("Error relieved on message consume: {}"),
  JMS_08("Commit threw error: {}"),
  JMS_09("Rollback threw error: {}"),
  JMS_10("Unknown message type '{}'"),
  JMS_11("Unable to create consumer: {}"),
  ;
  private final String msg;

  JmsErrors(String msg) {
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