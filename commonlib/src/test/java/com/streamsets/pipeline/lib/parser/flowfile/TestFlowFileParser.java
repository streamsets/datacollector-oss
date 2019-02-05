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
package com.streamsets.pipeline.lib.parser.flowfile;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.parser.*;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

public class TestFlowFileParser {

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  /**
    Since it requires a lot of steps to generate FlowFile with contents,
    we are testing only parsing failures here.
   */
  @Test(expected = DataParserException.class)
  public void testFlowFileParserFailure() throws Exception {
    // Inner data format factory
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.JSON);
    DataParserFactory jsonFactory = dataParserFactoryBuilder
        .setMaxDataLen(1000)
        .setMode(JsonMode.MULTIPLE_OBJECTS)
        .build();

    // Outer Data Format is FlowFile
    DataParserFactoryBuilder flowFileFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.FLOWFILE);
    DataParserFactory flowFileFactory = flowFileFactoryBuilder
        .setMaxDataLen(1000)
        .setConfig(FlowFileParserFactory.CONTENT_PARSER_FACTORY, jsonFactory)
        .build();

    String sampleContent = "This is a sample data to be retrieved";
    InputStream stream = new ByteArrayInputStream(sampleContent.getBytes(StandardCharsets.UTF_8));

    DataParser parser = flowFileFactory.getParser("ID", stream, "");
    parser.parse();
  }
}
