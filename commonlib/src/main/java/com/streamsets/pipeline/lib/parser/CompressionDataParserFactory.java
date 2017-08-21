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
package com.streamsets.pipeline.lib.parser;

import java.io.InputStream;
import java.io.Reader;

public class CompressionDataParserFactory extends DataParserFactory {

  private final DataParserFactory dataParserFactory;
  private final Settings settings;

  public CompressionDataParserFactory(Settings settings, DataParserFactory dataParserFactory) {
    super(settings);
    this.settings = settings;
    this.dataParserFactory = dataParserFactory;
  }

  @Override
  public DataParser getParser(String id, InputStream is, String offset) throws DataParserException {
    return new CompressionDataParser(id, is, offset, settings.getCompression(), settings.getFilePatternInArchive(),
        dataParserFactory);
  }

  @Override
  public DataParser getParser(String id, Reader reader, long offset) throws DataParserException {
    return dataParserFactory.getParser(id, reader, offset);
  }

}
