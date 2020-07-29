/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.devtest;

import com.codahale.metrics.Meter;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

@GenerateResourceBundle
@StageDef(
  version = 1,
  label = "Dev Random Record Source",
  description = "Generates records with the specified field names, using Long data. For development only.",
  execution = {ExecutionMode.STANDALONE, ExecutionMode.EDGE},
  icon = "dev.png",
  recordsByRef = true,
  upgraderDef = "upgrader/RandomSource.yaml",
  onlineHelpRefUrl ="index.html#datacollector/UserGuide/Pipeline_Design/DevStages.html"
)
public class RandomSource extends BaseSource {
  private static final Logger LOG = LoggerFactory.getLogger(RandomSource.class);
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "a,b,c",
      label = "Fields to Generate",
      displayMode = ConfigDef.DisplayMode.BASIC,
      description = "Name of the Long fields to generate. Enter a comma separated list."
  )
  public String fields;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Delay Between Batches",
      description = "Milliseconds to wait before sending the next batch",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 0,
      max = Integer.MAX_VALUE
  )
  public int delay;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "922337203685", // Long max value - 1
      label = "Max Records to Generate",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 0,
      max = Long.MAX_VALUE
  )
  public long maxRecordsToGenerate;

  private int batchCount;
  private int batchSize;
  private String[] fieldArr;
  private Random random;
  private Random randomNulls;
  private String[] lanes;
  private Meter randomMeter;
  private long recordsProduced;

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    fieldArr = fields.split(",");
    random = new Random();
    randomNulls = new Random();
    lanes = getContext().getOutputLanes().toArray(new String[getContext().getOutputLanes().size()]);
    randomMeter = getContext().createMeter("randomizer");
    return issues;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    //Capture snapshot generally provides a much smaller 'maxBatchSize'.
    //Without the below line, we may end up with a batch size value of the previous run
    //which could be greater than the record allowance for snapshot
    batchSize = maxBatchSize;
    if (batchCount++ % (random.nextInt(maxBatchSize) + 1) == 0) {
      batchSize = random.nextInt(maxBatchSize + 1);
    }

    if(delay > 0) {
        ThreadUtil.sleep(delay);
    }

    for (int i = 0; i < batchSize; i++ ) {
      if (recordsProduced >= maxRecordsToGenerate) {
        break;
      }
      batchMaker.addRecord(createRecord(lastSourceOffset, i), lanes[i % lanes.length]);
      recordsProduced++;
    }
    return "random";
  }

  private Record createRecord(String lastSourceOffset, int batchOffset) {
    Record record = getContext().createRecord("random:" + batchOffset);
    Map<String, Field> map = new LinkedHashMap<>();
    for (String field : fieldArr) {
      float randomFloat = randomNulls.nextFloat();
      if(randomFloat < 0.3) {
        map.put(field, Field.create(Field.Type.INTEGER, null));
        randomMeter.mark(0);
      } else {
        int randomValue = random.nextInt();
        map.put(field, Field.create(randomValue));
        randomMeter.mark(randomValue);
      }
    }
    record.set(Field.create(map));
    return record;
  }
}
