/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldfilter;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfig;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.configurablestage.DProcessor;

import java.util.List;

@StageDef(
    version="1.0.0",
    label="Field Remover",
    description="Removes fields from a record",
    icon="filter.png"
)
@ConfigGroups(Groups.class)
@HideConfig(onErrorRecord = true)
@GenerateResourceBundle
public class FieldFilterDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = Type.MODEL,
      defaultValue="REMOVE",
      label = "Action",
      description = "",
      displayPosition = 10,
      group = "FILTER"
  )
  @ValueChooser(FilterOperationChooserValues.class)
  public FilterOperation filterOperation;

  @ConfigDef(
      required = true,
      type = Type.MODEL,
      defaultValue="",
      label = "Fields",
      description = "",
      displayPosition = 20,
      group = "FILTER"
  )
  @FieldSelector
  public List<String> fields;

  @Override
  protected Processor createProcessor() {
    return new FieldFilterProcessor(filterOperation, fields);
  }

}