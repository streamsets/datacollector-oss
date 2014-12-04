/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.play;

import com.streamsets.pipeline.api.v1.AbstractRecordProcessor;
import com.streamsets.pipeline.api.v1.Configuration;
import com.streamsets.pipeline.api.v1.ModuleDef;
import com.streamsets.pipeline.api.v1.Record;
import com.streamsets.pipeline.api.v1.RecordEditor;

@ModuleDef(name = "MaskingProcessor", version = "0.1.0",
           description = "Masks specified record fields")
public class MaskingRecordProcessor extends AbstractRecordProcessor {
  private static final String MASK = "******";

  @Configuration(name = "fields-to-mask", type = Configuration.Type.STRING, defaultValue = {"ssn", "dob"})
  private String[] fieldsToMask;

  @Override
  protected Record process(Record record) {
    RecordEditor recordE = new RecordEditor(record);
    for (String field : fieldsToMask) {
      if (record.hasField(field)) {
        recordE.setValue(field, MASK);
      }
    }
    return record;
  }


}


