/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.clipper.stage;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;

import java.util.Random;

@StageDef(version="1.0.0", label="Random Record Source")
public class RandomSource extends BaseSource {

  @ConfigDef(required = true, type = ConfigDef.Type.STRING,
             label = "Record fields to generate, comma separated")
  public String fields;

  private String[] fieldArr;
  private Random random;

  @Override
  protected void init() throws StageException {
    fieldArr = fields.split(",");
    random = new Random();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    maxBatchSize = (maxBatchSize > -1) ? maxBatchSize : 10;
    for (int i = 0; i < maxBatchSize; i++ ) {
      batchMaker.addRecord(createRecord(lastSourceOffset, i));
    }
    return "random";
  }

  private Record createRecord(String lastSourceOffset, int batchOffset) {
    Record record = getContext().createRecord("random:" + batchOffset);
    for (String field : fieldArr) {
      record.set(Field.create(random.nextLong()));
    }
    return record;
  }
}
