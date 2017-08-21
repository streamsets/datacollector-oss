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
package com.streamsets.pipeline.lib.parser.avro;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.lib.util.SdcAvroTestUtil;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.streamsets.pipeline.config.OriginAvroSchemaSource.SOURCE;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_KEY;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_SOURCE_KEY;

public class TestAvroDataFileParser {

  @Test
  public void testAvroDataFileParser() throws Exception {
    File avroDataFile = SdcAvroTestUtil.createAvroDataFile();
    DataParser avroDataFileParser = getDataParser(avroDataFile, 1024, null);

    Record parse = avroDataFileParser.parse();
    Assert.assertNotNull(parse);
    Assert.assertEquals("244::1", avroDataFileParser.getOffset());
    Assert.assertNotNull(parse.getHeader().getAttribute(HeaderAttributeConstants.AVRO_SCHEMA));
    Assert.assertEquals(Schema.parseJson(SdcAvroTestUtil.AVRO_SCHEMA), Schema.parseJson(parse.getHeader().getAttribute(HeaderAttributeConstants.AVRO_SCHEMA)));

    parse = avroDataFileParser.parse();
    Assert.assertNotNull(parse);
    Assert.assertEquals("244::2", avroDataFileParser.getOffset());

    parse = avroDataFileParser.parse();
    Assert.assertNotNull(parse);
    Assert.assertEquals("244::3", avroDataFileParser.getOffset());

    parse = avroDataFileParser.parse();
    Assert.assertNull(parse);
    Assert.assertEquals("-1", avroDataFileParser.getOffset());

  }

  @Test
  public void testAvroDataFileParserOffset() throws Exception {
    File avroDataFile = SdcAvroTestUtil.createAvroDataFile();
    DataParser dataParser = getDataParser(avroDataFile, 1024, null);

    Record parse = dataParser.parse();
    Assert.assertNotNull(parse);
    Assert.assertEquals("244::1", dataParser.getOffset());

    dataParser = getDataParser(avroDataFile, 1024, dataParser.getOffset());
    parse = dataParser.parse();
    Assert.assertNotNull(parse);
    Assert.assertEquals("244::2", dataParser.getOffset());

    dataParser = getDataParser(avroDataFile, 1024, dataParser.getOffset());
    parse = dataParser.parse();
    Assert.assertNotNull(parse);
    Assert.assertEquals("244::3", dataParser.getOffset());

    dataParser = getDataParser(avroDataFile, 1024, dataParser.getOffset());
    parse = dataParser.parse();
    Assert.assertNull(parse);
    Assert.assertEquals("-1", dataParser.getOffset());

  }

  public static final String AVRO_SCHEMA = "{\n"
    +"\"type\": \"record\",\n"
    +"\"name\": \"Employee\",\n"
    +"\"fields\": [\n"
    +" {\"name\": \"name\", \"type\": \"string\"},\n"
    +" {\"name\": \"id\", \"type\": \"int\"}\n"
    +"]}";

  private static final String[] NAMES = {
    "Brock", "Hari"
  };

  @Test
  public void testIncorrectOffset() throws Exception {
    File avroDataFile = SdcAvroTestUtil.createAvroDataFile();
    avroDataFile.delete();
    Schema schema = new Schema.Parser().parse(AVRO_SCHEMA);
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(schema, avroDataFile);
    for (int i = 0; i < 5; i++) {
      GenericRecord r = new GenericData.Record(schema);
      r.put("name", NAMES[i % NAMES.length]);
      r.put("id", i);
      dataFileWriter.setSyncInterval(1073741824);
      dataFileWriter.append(r);
      dataFileWriter.sync();
    }
    dataFileWriter.flush();
    dataFileWriter.close();
    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(),
      DataParserFormat.AVRO);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(1024 * 1024)
        .setOverRunLimit(1000 * 1000)
        .setConfig(SCHEMA_SOURCE_KEY, SOURCE)
        .build();
    DataParser dataParser = factory.getParser(avroDataFile, null);
    Map<String, Record> records = new HashMap<>();
    Record record;
    while((record = dataParser.parse()) != null) {
      records.put(dataParser.getOffset(), record);
    }
    Assert.assertEquals(String.valueOf(records), 5, records.size());
    Assert.assertEquals(0, records.get("141::1").get("/id").getValueAsInteger());
    Assert.assertEquals(1, records.get("166::1").get("/id").getValueAsInteger());
    Assert.assertEquals(2, records.get("190::1").get("/id").getValueAsInteger());
    Assert.assertEquals(3, records.get("215::1").get("/id").getValueAsInteger());
    Assert.assertEquals(4, records.get("239::1").get("/id").getValueAsInteger());
  }

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR,
      Collections.<String>emptyList());
  }

  private DataParser getDataParser(File file, int maxObjectLength, String readerOffset) throws DataParserException {
    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(),
      DataParserFormat.AVRO);
    DataParserFactory factory = dataParserFactoryBuilder
      .setMaxDataLen(maxObjectLength)
      .setConfig(SCHEMA_KEY, SdcAvroTestUtil.AVRO_SCHEMA)
      .setOverRunLimit(1000)
      .build();
    return factory.getParser(file, readerOffset);
  }
}
