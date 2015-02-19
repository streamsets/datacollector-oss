/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.identity;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;

@GenerateResourceBundle
@StageDef(version = "1.0.0", label = "Identity",
          description = "It echoes every record it receives without changing, other than stage header information",
          icon="identity.png")
public class IdentityProcessor extends SingleLaneRecordProcessor {

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    batchMaker.addRecord(record);

  }

}
