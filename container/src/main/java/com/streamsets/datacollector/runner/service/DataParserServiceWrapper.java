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
package com.streamsets.datacollector.runner.service;

import com.streamsets.datacollector.util.LambdaUtil;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.service.dataformats.DataParser;
import com.streamsets.pipeline.api.service.dataformats.DataParserException;

import java.io.IOException;

public class DataParserServiceWrapper implements DataParser {

  private final ClassLoader classLoader;
  private final DataParser dataParser;

  public DataParserServiceWrapper(ClassLoader classLoader, DataParser dataParser) {
    this.classLoader = classLoader;
    this.dataParser = dataParser;
  }

  @Override
  public Record parse() throws IOException, DataParserException {
   return LambdaUtil.privilegedWithClassLoader(
      classLoader,
      IOException.class,
      DataParserException.class,
      dataParser::parse
    );
  }

  @Override
  public String getOffset() throws DataParserException, IOException {
    return LambdaUtil.privilegedWithClassLoader(
      classLoader,
      IOException.class,
      DataParserException.class,
      dataParser::getOffset
    );
  }

  @Override
  public void setTruncated() {
    LambdaUtil.privilegedWithClassLoader(
      classLoader,
      () -> { dataParser.setTruncated(); return null; }
    );
  }

  @Override
  public void close() throws IOException {
    LambdaUtil.privilegedWithClassLoader(
      classLoader,
      IOException.class,
      () -> { dataParser.close(); return null; }
    );
  }
}
