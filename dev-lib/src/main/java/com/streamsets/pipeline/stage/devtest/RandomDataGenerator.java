/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.devtest;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.BaseSource;

import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

@GenerateResourceBundle
@StageDef(version="1.0.0", label="Dev Data Generator",
  icon="random.svg")
public class RandomDataGenerator extends BaseSource {

  private Random random = new Random();

  @ConfigDef(label = "Fields to generate", required = false, type = ConfigDef.Type.MODEL, defaultValue="",
    description="Fields to generate of the indicated type")
  @ComplexField
  public List<DataGeneratorConfig> dataGenConfigs;

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    for(int i =0; i < maxBatchSize; i++) {
      batchMaker.addRecord(createRecord(lastSourceOffset, i));
    }
    return "random";
  }

  private Record createRecord(String lastSourceOffset, int batchOffset) {
    Record record = getContext().createRecord("random:" + batchOffset);
    Map<String, Field> map = new LinkedHashMap<>();
    for(DataGeneratorConfig dataGeneratorConfig : dataGenConfigs) {
      map.put(dataGeneratorConfig.field, Field.create(getFieldType(dataGeneratorConfig.type),
        generateRandomData(dataGeneratorConfig.type)));
    }
    record.set(Field.create(map));
    return record;
  }

  private Field.Type getFieldType(Type type) {
    switch(type) {
      case LONG:
        return Field.Type.LONG;
      case BOOLEAN:
        return Field.Type.BOOLEAN;
      case DOUBLE:
        return Field.Type.DOUBLE;
      case DATE:
        return Field.Type.DATE;
      case STRING:
        return Field.Type.STRING;
      case INTEGER:
        return Field.Type.INTEGER;
      case FLOAT:
        return Field.Type.FLOAT;
      case BYTE_ARRAY:
        return Field.Type.BYTE_ARRAY;
    }
    return Field.Type.STRING;
  }

  private Object generateRandomData(Type type) {
    switch(type) {
      case BOOLEAN :
        return random.nextBoolean();
      case DATE:
        return getRandomDate();
      case DOUBLE:
        return random.nextDouble();
      case FLOAT:
        return random.nextFloat();
      case INTEGER:
        return random.nextInt();
      case LONG:
        return random.nextLong();
      case STRING:
        return UUID.randomUUID().toString();
      case BYTE_ARRAY:
        return "StreamSets Inc, San Francisco".getBytes();
    }
    return null;
  }

  public Date getRandomDate() {
    GregorianCalendar gc = new GregorianCalendar();
    int year = randBetween(1990, 2010);
    gc.set(gc.YEAR, year);
    int dayOfYear = randBetween(1, gc.getActualMaximum(gc.DAY_OF_YEAR));
    gc.set(gc.DAY_OF_YEAR, dayOfYear);
    return gc.getTime();
  }

  public static int randBetween(int start, int end) {
    return start + (int)Math.round(Math.random() * (end - start));
  }

  public static class DataGeneratorConfig {

    @ConfigDef(required = true, type = ConfigDef.Type.STRING,
      label = "Record field to generate")
    public String field;

    @ConfigDef(required = true, type = ConfigDef.Type.MODEL,
      label = "Field Type",
      defaultValue = "STRING")
    @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = TypeChooserValueProvider.class)
    public Type type;
  }

  enum Type {
    STRING,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    DATE,
    BOOLEAN,
    BYTE_ARRAY
  }

}
