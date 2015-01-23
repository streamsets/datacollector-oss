/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.devtest;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestRandomDataGenerator {

  @Test
  public void testRandomDataGenerator() throws StageException {

    RandomDataGenerator.DataGeneratorConfig stringData = new RandomDataGenerator.DataGeneratorConfig();
    stringData.field = "name";
    stringData.type = RandomDataGenerator.Type.STRING;

    RandomDataGenerator.DataGeneratorConfig intData = new RandomDataGenerator.DataGeneratorConfig();
    intData.field = "age";
    intData.type = RandomDataGenerator.Type.INTEGER;

    RandomDataGenerator.DataGeneratorConfig longData = new RandomDataGenerator.DataGeneratorConfig();
    longData.field = "milliSecondsSinceBirth";
    longData.type = RandomDataGenerator.Type.LONG;

    RandomDataGenerator.DataGeneratorConfig dateData = new RandomDataGenerator.DataGeneratorConfig();
    dateData.field = "dob";
    dateData.type = RandomDataGenerator.Type.DATE;

    RandomDataGenerator.DataGeneratorConfig doubleData = new RandomDataGenerator.DataGeneratorConfig();
    doubleData.field = "salary";
    doubleData.type = RandomDataGenerator.Type.DOUBLE;


    SourceRunner runner = new SourceRunner.Builder(RandomDataGenerator.class)
      .addConfiguration("dataGenConfigs", ImmutableList.of(stringData, dateData, doubleData, longData, intData))
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
}
