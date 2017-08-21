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
package com.streamsets.pipeline.lib.parser.wholefile;

import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.io.fileref.FileRefTestUtil;
import com.streamsets.pipeline.lib.io.fileref.LocalFileRef;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class TestWholeFileDataParser {
  private File testDir;
  private Stage.Context context;

  @Before
  public void setup() throws Exception {
    testDir = new File("target", UUID.randomUUID().toString());
    testDir.mkdirs();
    FileRefTestUtil.writePredefinedTextToFile(testDir);
    context = ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  @After
  public void tearDown() throws Exception {
    testDir.delete();
  }


  @Test
  public void testParse() throws Exception {
    DataParserFactory factory = new DataParserFactoryBuilder(context, DataParserFormat.WHOLE_FILE)
        .setMaxDataLen(1000)
        .build();
    Map<String, Object> metadata = FileRefTestUtil.getFileMetadata(testDir);
    try (DataParser parser = factory.getParser(
        "id",
        metadata,
        FileRefTestUtil.getLocalFileRef(testDir, false, null, null)
    )) {
      Assert.assertEquals("0", parser.getOffset());
      Record record = parser.parse();
      Assert.assertNotNull(record);
      Assert.assertEquals("-1", parser.getOffset());
      Assert.assertTrue(record.has("/fileInfo"));
      Assert.assertTrue(record.has("/fileRef"));
      FileRef fileRef = record.get("/fileRef").getValueAsFileRef();
      Assert.assertTrue(fileRef instanceof LocalFileRef);
      InputStream is = fileRef.createInputStream(context, InputStream.class);
      byte[] b = new byte[FileRefTestUtil.TEXT.getBytes().length];
      int bytesRead = is.read(b);
      Assert.assertEquals(FileRefTestUtil.TEXT.getBytes().length, bytesRead);
      Assert.assertArrayEquals(FileRefTestUtil.TEXT.getBytes(StandardCharsets.UTF_8), b);
    }
  }

  @Test
  public void testMultipleParses() throws Exception {
    DataParserFactory factory = new DataParserFactoryBuilder(context, DataParserFormat.WHOLE_FILE)
        .setMaxDataLen(1000)
        .build();
    Map<String, Object> metadata = FileRefTestUtil.getFileMetadata(testDir);
    try (DataParser parser = factory.getParser(
        "id",
        metadata,
        FileRefTestUtil.getLocalFileRef(testDir, false, null, null)
    )) {
      Record record = parser.parse();
      Assert.assertNotNull(record);
      record = parser.parse();
      Assert.assertNull(record);
    }
  }

  @Test(expected = IOException.class)
  public void testClosedParser() throws Exception {
    DataParserFactory factory = new DataParserFactoryBuilder(context, DataParserFormat.WHOLE_FILE)
        .setMaxDataLen(1000)
        .build();
    Map<String, Object> metadata = FileRefTestUtil.getFileMetadata(testDir);
    DataParser parser = factory.getParser(
        "id",
        metadata,
        FileRefTestUtil.getLocalFileRef(testDir, false, null, null)
    );
    parser.close();
    parser.parse();
  }
}
