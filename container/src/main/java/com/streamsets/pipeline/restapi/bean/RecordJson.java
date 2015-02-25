/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.record.RecordImpl;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RecordJson {

  private final RecordImpl record;

  public RecordJson(RecordImpl record) {
    this.record = record;
  }

  @JsonCreator
  public RecordJson(@JsonProperty("header") HeaderJson headerJson,
                    @JsonProperty("value") FieldJson value) {
    record = new RecordImpl(BeanHelper.unwrapHeader(headerJson), BeanHelper.unwrapField(value));
  }

  public HeaderJson getHeader() {return new HeaderJson(record.getHeader());}

  public Object getValue() {
    return record.getValue();
  }

  @JsonIgnore
  public RecordImpl getRecord() {
    return record;
  }
}
