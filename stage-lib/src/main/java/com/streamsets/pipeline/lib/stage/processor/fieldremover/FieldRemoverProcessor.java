/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.fieldremover;

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

  @ConfigDef(label = "Fields to remove", required = true,type = Type.MODEL, defaultValue="",
    description="The fields which must be removed from the record. All other fields will be retained.")
  @FieldSelector
  public List<String> fields;

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Record recordClone = getContext().cloneRecord(record);
    for (String nameToRemove : fields) {
      recordClone.delete(nameToRemove);
    }
    batchMaker.addRecord(recordClone);
  }

}
