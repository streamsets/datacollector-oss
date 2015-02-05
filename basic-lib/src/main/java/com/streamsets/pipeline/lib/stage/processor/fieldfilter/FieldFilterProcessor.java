/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.fieldfilter;

import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;
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
@StageDef(
    version="1.0.0",
    label="Field Filter",
    description="???",
    icon="filter.png"
)
@ConfigGroups(FieldFilterProcessor.Groups.class)
public class FieldFilterProcessor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(FieldFilterProcessor.class);

  public enum Groups implements Label {
    FILTER;

    @Override
    public String getLabel() {
      return "Filter";
    }

  }

  public enum FilterOperation {
    KEEP("Keep Listed Fields"),
    REMOVE("Remove Listed Fields");

    private String label;

    private FilterOperation(String label) {
      this.label = label;
    }

    public String getLabel() {
      return label;
    }
  }

  @ConfigDef(
      required = true,
      type = Type.MODEL,
      defaultValue="KEEP",
      label = "Filter Operation",
      description = "???",
      displayPosition = 10,
      group = "FILTER"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = FilterOperationValues.class)
  public FilterOperation filterOperation;

  @ConfigDef(
      required = true,
      type = Type.MODEL,
      defaultValue="",
      label = "Fields",
      description = "???",
      displayPosition = 20,
      group = "FILTER"
  )
  @FieldSelector
  public List<String> fields;

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

}