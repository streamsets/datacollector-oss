/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.devtest;

import com.codahale.metrics.Meter;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

@GenerateResourceBundle
@StageDef(version="1.0.0", label="Random Record Source")
public class RandomSource extends BaseSource {

  @ConfigDef(required = true, type = ConfigDef.Type.STRING,
             label = "Record fields to generate, comma separated")
  public String fields;

  private int batchCount;
  private int batchSize;
  private String[] fieldArr;
  private Random random;
  private String[] lanes;
  private Meter randomMeter;

  @Override
  protected void init() throws StageException {
    fieldArr = fields.split(",");
    random = new Random();
    lanes = getContext().getOutputLanes().toArray(new String[getContext().getOutputLanes().size()]);
    randomMeter = getContext().createMeter("randomizer");
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    if (batchCount++ % 100 == 0) {
      batchSize = random.nextInt(maxBatchSize + 1);
    }
    for (int i = 0; i < batchSize; i++ ) {
      batchMaker.addRecord(createRecord(lastSourceOffset, i), lanes[i % lanes.length]);
    }
    return "random";
  }

  private Record createRecord(String lastSourceOffset, int batchOffset) {
    Record record = getContext().createRecord("random:" + batchOffset);
    Map<String, Field> map = new LinkedHashMap<>();
    for (String field : fieldArr) {
      long randomValue = random.nextLong();
      map.put(field, Field.create(randomValue));
      randomMeter.mark(randomValue);
    }
    record.set(Field.create(map));
    return record;
  }
}
