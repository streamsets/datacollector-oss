/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.datacollector.el;

import com.streamsets.datacollector.definition.ConcreteELDefinitionExtractor;
import com.streamsets.pipeline.lib.el.AvroEL;
import com.streamsets.pipeline.lib.el.RecordEL;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import org.apache.commons.codec.binary.Base64;

public class TestAvroEL {

  private static final String SCHEMA = "{\"namespace\":\"test\",\"type\":\"record\",\"name\":\"User\"," +
      "\"fields\":[{\"name\":\"name\",\"type\": \"string\"},{\"name\":\"id\",\"type\":\"int\"}]}";
  private static final String BASE64_ENCODED = "CGVtcDHIAQ==";
  private GenericRecord genericRecord;

  @Before
  public void setUp() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    genericRecord = new GenericData.Record(parser.parse(SCHEMA));
    genericRecord.put("name", "emp1");
    genericRecord.put("id", 100);
  }

  @Test
  public void testEL() throws Exception{
    ELEvaluator eval = new ELEvaluator("testBase64Decode", ConcreteELDefinitionExtractor.get(), AvroEL.class, Base64.class, RecordEL.class);
    ELVariables variables = new ELVariables();

    GenericRecord actualRecord = eval.evaluate(variables, "${avro:decode('"+SCHEMA+"',base64:decodeBytes('"+BASE64_ENCODED+"'))}",
        GenericRecord.class);

    Assert.assertEquals(actualRecord, genericRecord);
  }

}
