/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.external.stage;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldValueChooser;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseProcessor;
import com.streamsets.pipeline.api.ChooserMode;

import java.util.Map;

@StageDef(version="1.0", label="tc_processor"
  , description = "This processor lets the user select the types for the fields")
public class TypeConversionProcessor extends BaseProcessor {

  @FieldValueChooser(type = ChooserMode.PROVIDED, chooserValues =TypesProvider.class)
  @ConfigDef(type= ConfigDef.Type.MODEL, defaultValue = "",
    required = true, label = "field_to_type_map", description = "Contains the field and its target type as chosen by the user")
  public Map<String, String> fieldToTypeMap;

  @Override
  public void process(Batch batch, BatchMaker batchMaker) throws StageException {

  }
}
