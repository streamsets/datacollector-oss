/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class FieldJson {

  private final com.streamsets.pipeline.api.Field field;

  public FieldJson(com.streamsets.pipeline.api.Field field) {
    this.field = field;
  }

  @JsonIgnore
  public com.streamsets.pipeline.api.Field getField() {
    return field;
  }
}
