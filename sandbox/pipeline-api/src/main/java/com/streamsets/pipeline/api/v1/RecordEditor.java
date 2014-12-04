/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v1;

public class RecordEditor  {
  private Record record;

  public RecordEditor(Record record) {
    this.record = record;
  }

  public <T> void setValue(String fieldName, T value) {
    setValue(fieldName, record.getMetadata(fieldName), value);
  }

  public <T> void setValue(String fieldName, Metadata metadata, T value) {

  }

  public void removeField(String fieldName) {

  }

}
