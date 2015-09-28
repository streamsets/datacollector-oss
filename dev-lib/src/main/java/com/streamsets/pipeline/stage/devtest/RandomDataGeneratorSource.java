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
        generateRandomData(dataGeneratorConfig.type)));
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
        return "StreamSets Inc, San Francisco".getBytes(StandardCharsets.UTF_8);
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
      label = "Field Name")
    public String field;

    @ConfigDef(required = true, type = ConfigDef.Type.MODEL,
      label = "Field Type",
      defaultValue = "STRING")
    @ValueChooserModel(TypeChooserValueProvider.class)
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

  enum RootType {
    MAP,
    LIST_MAP
  }

}
