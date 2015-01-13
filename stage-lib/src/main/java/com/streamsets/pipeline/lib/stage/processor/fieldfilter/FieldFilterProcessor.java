/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.fieldfilter;

import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@GenerateResourceBundle
@StageDef( version="1.0.0", label="Filter")
public class FieldFilterProcessor extends SingleLaneRecordProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(FieldFilterProcessor.class);

  @ConfigDef(label = "Fields to filter", required = true, type = Type.MODEL, defaultValue="",
    description="The fields which must be retained or removed based on the filter option.")
  @FieldSelector
  public List<String> fields;

  @ConfigDef(label = "Filter Operation", required = true,type = Type.MODEL, defaultValue="KEEP",
    description="The filter operation on the selected fields")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = FilterOperationValues.class)
  public FilterOperation filterOperation;

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Set<String> fieldsToRemove = new HashSet<>();
    if(filterOperation == FilterOperation.REMOVE) {
      fieldsToRemove.addAll(fields);
    } else {
      fieldsToRemove.addAll(record.getFieldPaths());
      fieldsToRemove.removeAll(fields);
      fieldsToRemove.remove("");
    }
    for (String fieldToRemove : fieldsToRemove) {
      LOG.debug("Removing field {} from Record {}.", fieldsToRemove, record.getHeader().getSourceId());
      record.delete(fieldToRemove);
    }
    batchMaker.addRecord(record);
  }

  enum FilterOperation {
    KEEP,
    REMOVE
  }

}