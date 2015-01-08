/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.fieldfilter;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

@GenerateResourceBundle
@StageDef( version="1.0.0", label="Field Filter")
public class FieldFilterProcessor extends SingleLaneRecordProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(FieldFilterProcessor.class);

  @ConfigDef(label = "Fields to keep", required = true, type = Type.MODEL, defaultValue="",
    description="The fields which must be retained in the record. All other fields will be dropped.")

  @FieldSelector
  public List<String> fields;

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Record recordClone = getContext().cloneRecord(record);
    Set<String> fieldToRemove = recordClone.getFieldPaths();
    fieldToRemove.removeAll(fields);
    fieldToRemove.remove("");
    for (String nameToRemove : fieldToRemove) {
      LOG.debug("Removing field {} from Record {}.", fieldToRemove, record.getHeader().getSourceId());
      recordClone.delete(nameToRemove);
    }
    batchMaker.addRecord(recordClone);
  }

}