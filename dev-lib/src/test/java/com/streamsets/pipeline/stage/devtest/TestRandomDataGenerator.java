/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
}
