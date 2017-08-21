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
package com.streamsets.pipeline.lib.parser.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.DataCollectorServicesUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class TestJsonDataParserWithCompression {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  @BeforeClass
  public static void setUpClass() {
    DataCollectorServicesUtils.loadDefaultServices();
  }

  @Test
  public void testParseMultipleJson1() throws Exception {

    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.JSON);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(1000)
        .setMode(JsonMode.MULTIPLE_OBJECTS)
        .setCompression(Compression.COMPRESSED_ARCHIVE)
        .setFilePatternInArchive("*.txt")
        .build();

    String offset = "0";

    DataParser parser = factory.getParser("id", Resources.getResource("testArchive.tar.gz").openStream(), offset);
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));

    // testArchive.tar.gz contains 5 files file-0.txt - file-4.txt each with 3 Json string
    Record record;
    Map<String, Object> archiveInputOffset;

    for(int i = 0; i < 5; i++) {
      record = parser.parse();
      Assert.assertEquals("Hello", record.get().getValueAsList().get(0).getValueAsString());
      offset = parser.getOffset();
      archiveInputOffset = OBJECT_MAPPER.readValue(offset, Map.class);
      Assert.assertNotNull(archiveInputOffset);
      Assert.assertEquals("file-" + i + ".txt", archiveInputOffset.get("fileName"));
      Assert.assertEquals("10", archiveInputOffset.get("fileOffset"));

      record = parser.parse();
      Assert.assertEquals("Hi", record.get().getValueAsList().get(0).getValueAsString());
      offset = parser.getOffset();
      archiveInputOffset = OBJECT_MAPPER.readValue(offset, Map.class);
      Assert.assertNotNull(archiveInputOffset);
      Assert.assertEquals("file-" + i + ".txt", archiveInputOffset.get("fileName"));
      Assert.assertEquals("17", archiveInputOffset.get("fileOffset"));

      record = parser.parse();
      Assert.assertEquals("Bye", record.get().getValueAsList().get(0).getValueAsString());
      offset = parser.getOffset();
      archiveInputOffset = OBJECT_MAPPER.readValue(offset, Map.class);
      Assert.assertNotNull(archiveInputOffset);
      Assert.assertEquals("file-" + i + ".txt", archiveInputOffset.get("fileName"));
      Assert.assertEquals("24", archiveInputOffset.get("fileOffset"));
    }

    // Done reading the archive. Next attempt to parse should return null and offset "-1"
    Assert.assertNull(parser.parse());
    Assert.assertEquals("-1", parser.getOffset());

    parser.close();
  }

  @Test
  public void testParseMultipleJson2() throws Exception {

    //This test is different from the above. The parser is recreated before every read. This mimicks stop and restart
    DataParserFactoryBuilder dataParserFactoryBuilder =
      new DataParserFactoryBuilder(getContext(), DataParserFormat.JSON);
    DataParserFactory factory = dataParserFactoryBuilder
      .setMaxDataLen(1000)
      .setMode(JsonMode.MULTIPLE_OBJECTS)
      .setCompression(Compression.COMPRESSED_ARCHIVE)
      .setFilePatternInArchive("*.txt")
      .build();

    String offset = "0";
    DataParser parser = factory.getParser("id", Resources.getResource("testArchive.tar.gz").openStream(), offset);
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));

    // testArchive.tar.gz contains 5 files file-0.txt - file-4.txt each with 3 Json string
    Record record;
    Map<String, Object> archiveInputOffset;

    for(int i = 0; i < 5; i++) {
      parser = factory.getParser("id",Resources.getResource("testArchive.tar.gz").openStream(), offset);
      record = parser.parse();
      Assert.assertEquals("Hello", record.get().getValueAsList().get(0).getValueAsString());
      offset = parser.getOffset();
      archiveInputOffset = OBJECT_MAPPER.readValue(offset, Map.class);
      Assert.assertNotNull(archiveInputOffset);
      Assert.assertEquals("file-" + i + ".txt", archiveInputOffset.get("fileName"));
      Assert.assertEquals("10", archiveInputOffset.get("fileOffset"));

      parser = factory.getParser("id", Resources.getResource("testArchive.tar.gz").openStream(), offset);
      record = parser.parse();
      Assert.assertEquals("Hi", record.get().getValueAsList().get(0).getValueAsString());
      offset = parser.getOffset();
      archiveInputOffset = OBJECT_MAPPER.readValue(offset, Map.class);
      Assert.assertNotNull(archiveInputOffset);
      Assert.assertEquals("file-" + i + ".txt", archiveInputOffset.get("fileName"));
      Assert.assertEquals("17", archiveInputOffset.get("fileOffset"));

      parser = factory.getParser("id", Resources.getResource("testArchive.tar.gz").openStream(), offset);
      record = parser.parse();
      Assert.assertEquals("Bye", record.get().getValueAsList().get(0).getValueAsString());
      offset = parser.getOffset();
      archiveInputOffset = OBJECT_MAPPER.readValue(offset, Map.class);
      Assert.assertNotNull(archiveInputOffset);
      Assert.assertEquals("file-" + i + ".txt", archiveInputOffset.get("fileName"));
      Assert.assertEquals("24", archiveInputOffset.get("fileOffset"));
    }

    parser = factory.getParser("id", Resources.getResource("testArchive.tar.gz").openStream(), offset);
    Assert.assertNull(parser.parse());
    Assert.assertEquals("-1", parser.getOffset());

    parser.close();
  }

}
