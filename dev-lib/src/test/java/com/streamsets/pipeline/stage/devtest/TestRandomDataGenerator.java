/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestRandomDataGenerator {

  @Test
  public void testRandomDataGenerator() throws StageException {

    RandomDataGeneratorSource.DataGeneratorConfig stringData = new RandomDataGeneratorSource.DataGeneratorConfig();
    stringData.field = "name";
    stringData.type = RandomDataGeneratorSource.Type.STRING;

    RandomDataGeneratorSource.DataGeneratorConfig intData = new RandomDataGeneratorSource.DataGeneratorConfig();
    intData.field = "age";
    intData.type = RandomDataGeneratorSource.Type.INTEGER;

    RandomDataGeneratorSource.DataGeneratorConfig longData = new RandomDataGeneratorSource.DataGeneratorConfig();
    longData.field = "milliSecondsSinceBirth";
    longData.type = RandomDataGeneratorSource.Type.LONG;

    RandomDataGeneratorSource.DataGeneratorConfig dateData = new RandomDataGeneratorSource.DataGeneratorConfig();
    dateData.field = "dob";
    dateData.type = RandomDataGeneratorSource.Type.DATE;

    RandomDataGeneratorSource.DataGeneratorConfig doubleData = new RandomDataGeneratorSource.DataGeneratorConfig();
    doubleData.field = "salary";
    doubleData.type = RandomDataGeneratorSource.Type.DOUBLE;


    SourceRunner runner = new SourceRunner.Builder(RandomDataGeneratorSource.class)
      .addConfiguration("dataGenConfigs", Arrays.asList(stringData, dateData, doubleData, longData, intData))
      .addConfiguration("rootFieldType", RandomDataGeneratorSource.RootType.MAP)
      .addOutputLane("a")
      .build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce(null, 1);
      List<Record> records = output.getRecords().get("a");
      Assert.assertTrue(records.size() == 1);
      Assert.assertEquals(Field.Type.STRING, records.get(0).get("/name").getType());
      Assert.assertEquals(Field.Type.INTEGER, records.get(0).get("/age").getType());
      Assert.assertEquals(Field.Type.LONG, records.get(0).get("/milliSecondsSinceBirth").getType());
      Assert.assertEquals(Field.Type.DATE, records.get(0).get("/dob").getType());
      Assert.assertEquals(Field.Type.DOUBLE, records.get(0).get("/salary").getType());
    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testLongSequence() throws StageException {
    RandomDataGeneratorSource.DataGeneratorConfig seq = new RandomDataGeneratorSource.DataGeneratorConfig();
    seq.field = "id";
    seq.type = RandomDataGeneratorSource.Type.LONG_SEQUENCE;

    SourceRunner runner = new SourceRunner.Builder(RandomDataGeneratorSource.class)
      .addConfiguration("dataGenConfigs", Arrays.asList(seq))
      .addConfiguration("rootFieldType", RandomDataGeneratorSource.RootType.MAP)
      .addOutputLane("a")
      .build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce(null, 1000);
      List<Record> records = output.getRecords().get("a");
      Assert.assertTrue(records.size() > 1);
      for(long i = 0; i < records.size(); i++) {
        Field field = records.get((int)i).get().getValueAsMap().get("id");
        Assert.assertNotNull(field);
        Assert.assertEquals(Field.Type.LONG, field.getType());
        Assert.assertEquals(i, field.getValueAsLong());
      }
    } finally {
      runner.runDestroy();
    }
  }
}
