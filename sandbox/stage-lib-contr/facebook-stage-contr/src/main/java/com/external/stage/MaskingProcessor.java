/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.external.stage;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseProcessor;

import java.util.List;

@StageDef(version="1.0", label="masking_processor"
, description = "This processor masks the fields selected by the user")
public class MaskingProcessor extends BaseProcessor {

  @FieldSelector
  @ConfigDef(type= ConfigDef.Type.MODEL, defaultValue = "",
    required = true, label = "fields_to_mask", description = "Indicates the fields to be masked")
  public List<String> fieldsToMask;

  @Override
  public void process(Batch batch, BatchMaker batchMaker) throws StageException {

  }
}
