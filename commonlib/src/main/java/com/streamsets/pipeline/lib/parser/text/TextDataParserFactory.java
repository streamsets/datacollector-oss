/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.parser.text;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class TextDataParserFactory extends DataParserFactory {
  public static final String MULTI_LINE_KEY = "multiLines";
  public static final boolean MULTI_LINE_DEFAULT = false;

  public static final Map<String, Object> CONFIGS = ImmutableMap.<String, Object>of(MULTI_LINE_KEY, MULTI_LINE_DEFAULT);
  public static final Set<Class<? extends Enum>> MODES = Collections.emptySet();

  static final String TEXT_FIELD_NAME = "text";
  static final String TRUNCATED_FIELD_NAME = "truncated";


  public TextDataParserFactory(Settings settings) {
    super(settings);
  }

  @Override
  public DataParser getParser(String id, InputStream is, long offset) throws DataParserException {
    return createParser(id, createReader(is), offset);
  }

  @Override
  public DataParser getParser(String id, Reader reader, long offset) throws DataParserException {
    return createParser(id, createReader(reader), offset);
  }

  private DataParser createParser(String id, OverrunReader reader, long offset) throws DataParserException {
    Utils.checkState(reader.getPos() == 0, Utils.formatL("reader must be in position '0', it is at '{}'",
      reader.getPos()));
    try {
      return new TextCharDataParser(getSettings().getContext(), id, (boolean) getSettings().getConfig(MULTI_LINE_KEY),
                                    reader, offset, getSettings().getMaxRecordLen(), TEXT_FIELD_NAME,
                                    TRUNCATED_FIELD_NAME);
    } catch (IOException ex) {
      throw new DataParserException(Errors.TEXT_PARSER_00, id, offset, ex.toString(), ex);
    }
  }

}
