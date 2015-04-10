/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.scripting;

import com.streamsets.pipeline.api.Record;

public class ScriptRecord {
  final Record record;
  public Object value;

  ScriptRecord(Record record, Object scriptObject) {
    this.record = record;
    value = scriptObject;
  }

}
