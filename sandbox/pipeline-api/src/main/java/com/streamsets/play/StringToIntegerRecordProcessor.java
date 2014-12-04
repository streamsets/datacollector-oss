/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.play;

import com.streamsets.pipeline.api.v1.AbstractRecordProcessor;
import com.streamsets.pipeline.api.v1.Configuration;
import com.streamsets.pipeline.api.v1.Metadata;
import com.streamsets.pipeline.api.v1.Metadata.Builder;
import com.streamsets.pipeline.api.v1.Metadata.Type;
import com.streamsets.pipeline.api.v1.ModuleDef;
import com.streamsets.pipeline.api.v1.Record;
import com.streamsets.pipeline.api.v1.RecordEditor;

@ModuleDef(name = "TypeConvertorProcessor", version = "0.1.0",
           description = "Converts String fields to other types")
public class StringToIntegerRecordProcessor extends AbstractRecordProcessor {

  @Configuration(name = "fields-to-handle", type = Configuration.Type.STRING, defaultValue = {"age", "weight"})
  private String[] fieldsToConvert;


  private Metadata[] fieldsMetadata;

  @Override
  protected void init() {
    fieldsMetadata = new Metadata[fieldsToConvert.length];
    Metadata.Builder builder = new Metadata.Builder();
    builder.setFormat("%d");
    builder.setType(Type.INTEGER);
    for (int i = 0; i < fieldsMetadata.length; i++) {
      String field = fieldsToConvert[i];
      builder.setName(field);
      fieldsMetadata[i] = builder.build();
    }
  }

  @Override
  protected Record process(Record record) {
    RecordEditor recordE = new RecordEditor(record);
    for (Metadata fieldMetada : fieldsMetadata) {
      if (record.hasField(fieldMetada.getName())) {
        recordE.setValue(fieldMetada.getName(), fieldMetada,
                         Integer.parseInt(record.getValue(fieldMetada.getName(), String.class)));
      }
    }
    return record;
  }


}


