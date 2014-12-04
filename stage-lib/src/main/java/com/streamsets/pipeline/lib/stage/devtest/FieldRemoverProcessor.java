/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.devtest;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;

import java.util.List;

@GenerateResourceBundle
@StageDef( version="1.0.0", label="Field Remover")
public class FieldRemoverProcessor extends SingleLaneRecordProcessor {

  @ConfigDef(label = "Fields to remove", required = true,type = Type.MODEL, defaultValue="")
  @FieldSelector
  public List<String> fields;

  // the annotations processor will fail if variable is not List

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    for (String nameToRemove : fields) {
      record.delete(nameToRemove);
    }
    batchMaker.addRecord(record);
  }

}
