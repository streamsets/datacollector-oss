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

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.data.DataFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.origin.lib.FlowFileVersion;
import org.apache.nifi.util.FlowFileUnpackager;
import org.apache.nifi.util.FlowFileUnpackagerV3;
import org.apache.nifi.util.FlowFileUnpackagerV2;
import org.apache.nifi.util.FlowFileUnpackagerV1;

import java.io.InputStream;
import java.io.Reader;

import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

public class FlowFileParserFactory extends DataParserFactory {

  public static final Map<String, Object> CONFIGS = new HashMap<>();
  public static final Set<Class<? extends Enum>> MODES = Collections.emptySet();
  public final static String CONTENT_PARSER_FACTORY = "contentParserFactory";
  public final static String FLOWFILE_VERSION = "flowfileVersion";

  private final DataParserFactory contentParserFactory;
  private final FlowFileUnpackager unpacker;

  static {
    // The ParserFactory for the content will be filled later. So putting dummy for init
    CONFIGS.put(CONTENT_PARSER_FACTORY, new Object());
    CONFIGS.put(FLOWFILE_VERSION, FlowFileVersion.FLOWFILE_V3);
  }

  public FlowFileParserFactory(DataFactory.Settings settings) {
    super(settings);

    contentParserFactory = (DataParserFactory)settings.getConfig(CONTENT_PARSER_FACTORY);
    FlowFileVersion version = (FlowFileVersion)settings.getConfig(FLOWFILE_VERSION);
    switch (version) {
      case FLOWFILE_V1:
        unpacker = new FlowFileUnpackagerV1();
        break;
      case FLOWFILE_V2:
        unpacker = new FlowFileUnpackagerV2();
        break;
      case FLOWFILE_V3:
        unpacker = new FlowFileUnpackagerV3();
        break;
      default: // should not happen
        throw new UnsupportedOperationException("Unsupported FlowFile Version");
    }
  }

  @Override
  public DataParser getParser(String id, InputStream is, String offset) throws DataParserException {
    return new FlowFileParser(getSettings().getContext(), id, is, unpacker, contentParserFactory);
  }

  @Override
  public DataParser getParser(String id, Reader reader, long offset) throws DataParserException {
    throw new UnsupportedOperationException(Utils.format("{} does not support character based data", getClass().getName()));
  }

}
