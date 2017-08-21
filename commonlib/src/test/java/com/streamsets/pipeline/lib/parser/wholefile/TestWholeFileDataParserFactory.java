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

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.io.fileref.FileRefTestUtil;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class TestWholeFileDataParserFactory {
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
  public void testWholeFileDataParserFactory() throws Exception {
    DataParserFactory factory = new DataParserFactoryBuilder(context, DataParserFormat.WHOLE_FILE)
        .setMaxDataLen(1000)
        .build();
    Map<String, Object> metadata = FileRefTestUtil.getFileMetadata(testDir);
    try (DataParser parser = factory.getParser(
        "id",
        metadata,
        FileRefTestUtil.getLocalFileRef(testDir, false, null, null))
    ) {
      Assert.assertEquals("0", parser.getOffset());
    }
  }


  private void getParserWithInvalidMetadata(
      FileRef fileRef,
      Map<String, Object> metadata,
      DataParserFactory dataParserFactory,
      ErrorCode errorCode
  ) {
    try {
      dataParserFactory.getParser("id", metadata, fileRef);
      Assert.fail("getParser should fail for missing mandatory metadata");
    } catch (DataParserException e) {
      Assert.assertEquals(errorCode, e.getErrorCode());
    }
  }

  @Test
  public void testWholeFileDataParserFactoryMissingMetadata() throws Exception {
    DataParserFactory factory = new DataParserFactoryBuilder(context, DataParserFormat.WHOLE_FILE)
        .setMaxDataLen(1000)
        .build();
    Map<String, Object> metadata = FileRefTestUtil.getFileMetadata(testDir);
    FileRef fileRef = FileRefTestUtil.getLocalFileRef(testDir, false, null, null);
    Set<String> keys = new HashSet<>(FileRefUtil.MANDATORY_METADATA_INFO);
    for (String key :keys) {
      Map<String, Object> metadataCopy = new HashMap<>(metadata);
      metadataCopy.remove(key);
      getParserWithInvalidMetadata(fileRef, metadataCopy, factory, Errors.WHOLE_FILE_PARSER_ERROR_0);
    }
  }

  @Test(expected = NullPointerException.class)
  public void testNullFileRef() throws Exception {
    DataParserFactory factory = new DataParserFactoryBuilder(context, DataParserFormat.WHOLE_FILE)
        .setMaxDataLen(1000)
        .build();
    Map<String, Object> metadata = FileRefTestUtil.getFileMetadata(testDir);
    factory.getParser("id", metadata, null);
  }
}
