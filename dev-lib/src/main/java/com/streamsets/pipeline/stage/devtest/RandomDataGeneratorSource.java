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

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.BaseSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

@GenerateResourceBundle
@StageDef(
  version = 3,
  label="Dev Data Generator",
  description = "Generates records with the specified field names based on the selected data type. For development only.",
  execution = ExecutionMode.STANDALONE,
  icon= "dev.png",
  upgrader = RandomDataGeneratorSourceUpgrader.class,
    onlineHelpRefUrl = "index.html#Pipeline_Design/DevStages.html"
)
public class RandomDataGeneratorSource extends BaseSource {

  private final Random random = new Random();

  @ConfigDef(label = "Fields to Generate", required = false, type = ConfigDef.Type.MODEL, defaultValue="",
    description="Fields to generate of the indicated type")
  @ListBeanModel
  public List<DataGeneratorConfig> dataGenConfigs;

  @ConfigDef(label = "Root Field Type",
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "MAP",
    description = "Field Type for root object")
  @ValueChooserModel(RootTypeChooserValueProvider.class)
  public RootType rootFieldType;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.MAP,
    label = "Header Attributes",
    description = "Attributes to be put in the generated record header"
  )
  public Map<String, String> headerAttributes;

  /**
   * Counter for LONG_SEQUENCE type
   */
  private long counter;

  @Override
  protected List<ConfigIssue> init() {
    counter = 0;
    return super.init();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    for(int i =0; i < maxBatchSize; i++) {
      batchMaker.addRecord(createRecord(lastSourceOffset, i));
    }
    return "random";
  }

  private Record createRecord(String lastSourceOffset, int batchOffset) {
    Record record = getContext().createRecord("random:" + batchOffset);
    if(headerAttributes != null && !headerAttributes.isEmpty()) {
      for (Map.Entry<String, String> e : headerAttributes.entrySet()) {
        record.getHeader().setAttribute(e.getKey(), e.getValue());
      }
    }
    LinkedHashMap<String, Field> map = new LinkedHashMap<>();
    for(DataGeneratorConfig dataGeneratorConfig : dataGenConfigs) {
      map.put(dataGeneratorConfig.field, Field.create(getFieldType(dataGeneratorConfig.type),
        generateRandomData(dataGeneratorConfig)));
    }

    switch (rootFieldType) {
      case MAP:
        record.set(Field.create(map));
        break;
      case LIST_MAP:
        record.set(Field.createListMap(map));
        break;
    }

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
      case DATETIME:
        return Field.Type.DATETIME;
      case TIME:
        return Field.Type.TIME;
      case STRING:
        return Field.Type.STRING;
      case INTEGER:
        return Field.Type.INTEGER;
      case FLOAT:
        return Field.Type.FLOAT;
      case DECIMAL:
        return Field.Type.DECIMAL;
      case BYTE_ARRAY:
        return Field.Type.BYTE_ARRAY;
      case LONG_SEQUENCE:
        return Field.Type.LONG;
    }
    return Field.Type.STRING;
  }

  private Object generateRandomData(DataGeneratorConfig config) {
    switch(config.type) {
      case BOOLEAN :
        return random.nextBoolean();
      case DATE:
        return getRandomDate();
      case DATETIME:
        return getRandomDateTime();
      case TIME:
        return getRandomTime();
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
      case DECIMAL:
        return new BigDecimal(BigInteger.valueOf(random.nextLong() % (long)Math.pow(10, config.precision)), config.scale);
      case BYTE_ARRAY:
        return "StreamSets Inc, San Francisco".getBytes(StandardCharsets.UTF_8);
      case LONG_SEQUENCE:
        return counter++;
    }
    return null;
  }

  public Date getRandomDate() {
    GregorianCalendar gc = new GregorianCalendar();
    gc.set(
      randBetween(1990, 2020),
      randBetween(1, gc.getActualMaximum(gc.MONTH)),
      randBetween(1, gc.getActualMaximum(gc.DAY_OF_MONTH)),
      0, 0, 0
    );
    return gc.getTime();
  }

  public Date getRandomTime() {
    GregorianCalendar gc = new GregorianCalendar();
    gc.set(
      1970, 0, 1,
      randBetween(0, gc.getActualMaximum(gc.HOUR_OF_DAY)),
      randBetween(0, gc.getActualMaximum(gc.MINUTE)),
      randBetween(0, gc.getActualMaximum(gc.SECOND))
    );
    return gc.getTime();
  }

  public Date getRandomDateTime() {
    GregorianCalendar gc = new GregorianCalendar();
    gc.set(
      randBetween(1990, 2020),
      randBetween(1, gc.getActualMaximum(gc.MONTH)),
      randBetween(1, gc.getActualMaximum(gc.DAY_OF_MONTH)),
      randBetween(0, gc.getActualMaximum(gc.HOUR_OF_DAY)),
      randBetween(0, gc.getActualMaximum(gc.MINUTE)),
      randBetween(0, gc.getActualMaximum(gc.SECOND))
    );
    return gc.getTime();
  }

  public static int randBetween(int start, int end) {
    return start + (int)Math.round(Math.random() * (end - start));
  }

  public static class DataGeneratorConfig {

    @ConfigDef(required = true, type = ConfigDef.Type.STRING,
      label = "Field Name")
    public String field;

    @ConfigDef(required = true, type = ConfigDef.Type.MODEL,
      label = "Field Type",
      defaultValue = "STRING")
    @ValueChooserModel(TypeChooserValueProvider.class)
    public Type type;

    @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "10",
      label = "Precision",
      description = "Precision of the generated decimal.",
      min = 0,
      dependsOn = "type",
      triggeredByValue = "DECIMAL"
    )
    public long precision;

    @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "2",
      label = "scale",
      description = "Scale of the generated decimal.",
      min = 0,
      dependsOn = "type",
      triggeredByValue = "DECIMAL"
    )
    public int scale;

  }

  enum Type {
    STRING,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    DATE,
    DATETIME,
    TIME,
    BOOLEAN,
    DECIMAL,
    BYTE_ARRAY,
    LONG_SEQUENCE
  }

  enum RootType {
    MAP,
    LIST_MAP
  }

}
