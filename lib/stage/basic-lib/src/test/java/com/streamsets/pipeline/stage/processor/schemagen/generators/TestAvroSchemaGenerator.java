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
package com.streamsets.pipeline.stage.processor.schemagen.generators;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import com.streamsets.pipeline.stage.processor.schemagen.config.AvroDefaultConfig;
import com.streamsets.pipeline.stage.processor.schemagen.config.AvroType;
import com.streamsets.pipeline.stage.processor.schemagen.config.SchemaGeneratorConfig;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Date;

import static org.powermock.api.mockito.PowerMockito.mock;

public class TestAvroSchemaGenerator {

  private SchemaGeneratorConfig config;
  private AvroSchemaGenerator generator;

  @Before
  public void setUp() {
    config = new SchemaGeneratorConfig();
    config.schemaName = "test_schema";
    generator = new AvroSchemaGenerator();
    generator.init(config, mock(Stage.Context.class));
  }

  public void generateAndValidateSchema(Record record, String fieldFragment) throws OnRecordErrorException {
    String schema = generator.generateSchema(record);
    Assert.assertNotNull(schema);
    Schema expectedSchema = Schema.parse("{\"type\":\"record\",\"name\":\"test_schema\",\"fields\":[" + fieldFragment + "]}");
    Assert.assertEquals(expectedSchema, Schema.parse(schema));
  }

  @Test
  public void testGenerateSimpleSchema() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
      "a", Field.create(Field.Type.STRING, "Arvind"),
      "b", Field.create(Field.Type.BOOLEAN, true),
      "c", Field.create(Field.Type.INTEGER, 0)
    )));

    generateAndValidateSchema(
        record,
        "{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":\"boolean\"},{\"name\":\"c\",\"type\":\"int\"}"
    );
  }

  @Test
  public void testGenerateSimpleNullableSchema() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
      "name", Field.create(Field.Type.STRING, "Bryan"),
      "salary", Field.create(Field.Type.INTEGER, 10)
    )));

    this.config.avroNullableFields = true;

    generateAndValidateSchema(
      record,
      "{\"name\":\"name\",\"type\":[\"null\",\"string\"]},{\"name\":\"salary\",\"type\":[\"null\",\"int\"]}"
    );
  }

  @Test
  public void testGenerateSimpleNullableSchemaDefaultToNull() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
      "name", Field.create(Field.Type.STRING, "Bryan"),
      "salary", Field.create(Field.Type.INTEGER, 10)
    )));

    this.config.avroNullableFields = true;
    this.config.avroDefaultNullable = true;

    generateAndValidateSchema(
      record,
      "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"salary\",\"type\":[\"null\",\"int\"],\"default\":null}"
    );
  }

  @Test
  public void testGenerateSchemaDefaultForString() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
      "name", Field.create(Field.Type.STRING, "Bryan")
    )));

    this.config.avroDefaultTypes = ImmutableList.of(new AvroDefaultConfig(AvroType.STRING, "defaultValue"));
    this.generator.init(config, mock(Stage.Context.class));

    generateAndValidateSchema(
      record,
      "{\"name\":\"name\",\"type\":\"string\",\"default\":\"defaultValue\"}"
    );
  }

  @Test
  public void testGenerateSchemaDefaultForFloat() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
      "float", Field.create(Field.Type.FLOAT, 1.0f)
    )));

    this.config.avroDefaultTypes = ImmutableList.of(new AvroDefaultConfig(AvroType.FLOAT, "666.0"));
    this.generator.init(config, mock(Stage.Context.class));

    generateAndValidateSchema(
      record,
      "{\"name\":\"float\",\"type\":\"float\",\"default\":666.0}"
    );
  }
  @Test
  public void testGenerateDecimal() throws OnRecordErrorException {
    Field decimal = Field.create(Field.Type.DECIMAL, new BigDecimal("10.2"));
    decimal.setAttribute(HeaderAttributeConstants.ATTR_PRECISION, "3");
    decimal.setAttribute(HeaderAttributeConstants.ATTR_SCALE, "1");

    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
      "decimal", decimal
    )));

    generateAndValidateSchema(
      record,
      "{\"name\":\"decimal\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":3,\"scale\":1}}"
    );
  }

  @Test
  public void testGenerateDate() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
      "date", Field.create(Field.Type.DATE, new Date())
    )));

    generateAndValidateSchema(
      record,
      "{\"name\":\"date\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}}"
    );
  }

  @Test
  public void testGenerateTime() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
      "time", Field.create(Field.Type.TIME, new Date())
    )));

    generateAndValidateSchema(
      record,
      "{\"name\":\"time\",\"type\":{\"type\":\"int\",\"logicalType\":\"time-millis\"}}"
    );
  }

  @Test
  public void testGenerateDateTime() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
      "datetime", Field.create(Field.Type.DATETIME, new Date())
    )));

    generateAndValidateSchema(
      record,
      "{\"name\":\"datetime\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}"
    );
  }

  @Test
  public void testGenerateList() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
      "list", Field.create(Field.Type.LIST, ImmutableList.of(
          Field.create(Field.Type.STRING, "Arvind"),
          Field.create(Field.Type.STRING, "Girish")
      )
    ))));

    generateAndValidateSchema(
      record,
      "{\"name\":\"list\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}"
    );
  }

  @Test
  public void testGenerateMap() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
      "map", Field.create(Field.Type.MAP, ImmutableMap.of(
          "Doer", Field.create(Field.Type.STRING, "Arvind"),
          "Talker", Field.create(Field.Type.STRING, "Girish")
      )
    ))));

    generateAndValidateSchema(
      record,
      "{\"name\":\"map\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}"
    );
  }

  @Test
  public void testGenerateMapWithDefaultValues() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
      "map", Field.create(Field.Type.MAP, ImmutableMap.of(
          "Doer", Field.create(Field.Type.STRING, "Arvind"),
          "Talker", Field.create(Field.Type.STRING, "Girish")
      )
    ))));

    this.config.avroDefaultTypes = ImmutableList.of(new AvroDefaultConfig(AvroType.STRING, "defaultValue"));
    this.generator.init(config, mock(Stage.Context.class));

    generateAndValidateSchema(
      record,
      "{\"name\":\"map\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"string\",\"defaultValue\":\"defaultValue\"}}}"
    );
  }

  @Test(expected = OnRecordErrorException.class)
  public void testGenerateMapNull() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
      "map", Field.create(Field.Type.MAP, null
    ))));

    this.config.avroDefaultTypes = ImmutableList.of(new AvroDefaultConfig(AvroType.STRING, "defaultValue"));
    this.generator.init(config, mock(Stage.Context.class));

    generateAndValidateSchema(record,null);
  }

  @Test(expected = OnRecordErrorException.class)
  public void testGenerateMapEmpty() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
      "map", Field.create(Field.Type.MAP, Collections.emptyMap()
    ))));

    this.config.avroDefaultTypes = ImmutableList.of(new AvroDefaultConfig(AvroType.STRING, "defaultValue"));
    this.generator.init(config, mock(Stage.Context.class));

    generateAndValidateSchema(record,null);
  }

  @Test
  public void testGenerateExpandedTypes() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
      "short", Field.create(Field.Type.SHORT, 10),
      "char", Field.create(Field.Type.CHAR, 'A')
    )));

    this.config.avroExpandTypes = true;

    generateAndValidateSchema(
      record,
      "{\"name\":\"short\",\"type\":\"int\"},{\"name\":\"char\",\"type\":\"string\"}"
    );
  }
}
