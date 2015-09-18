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
package com.streamsets.pipeline.lib.parser.avro;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.Errors;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class AvroDataParserFactory extends DataParserFactory {

  static final String KEY_PREFIX = "avro.";
  public static final String SCHEMA_KEY = KEY_PREFIX + "schema";
  static final String SCHEMA_DEFAULT = "";
  public static final String SCHEMA_IN_MESSAGE_KEY = KEY_PREFIX + "schemaInMessage";
  static final boolean SCHEMA_IN_MESSAGE_DEFAULT = false;

  public static final Map<String, Object> CONFIGS;

  static {
    Map<String, Object> configs = new HashMap<>();
    configs.put(SCHEMA_KEY, SCHEMA_DEFAULT);
    configs.put(SCHEMA_IN_MESSAGE_KEY, SCHEMA_IN_MESSAGE_DEFAULT);
    CONFIGS = Collections.unmodifiableMap(configs);
  }

  @SuppressWarnings("unchecked")
  public static final Set<Class<? extends Enum>> MODES = (Set) ImmutableSet.of();

  private final String schema;
  private final boolean schemaInMessage;

  public AvroDataParserFactory(Settings settings) {
    super(settings);
    schema = settings.getConfig(SCHEMA_KEY);
    schemaInMessage = settings.getConfig(SCHEMA_IN_MESSAGE_KEY);
    Utils.checkNotNull(schema, "Avro Schema");
  }

  @Override
  public DataParser getParser(String id, InputStream is, long offset) throws DataParserException {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataParser getParser(String id, Reader reader, long offset) throws DataParserException {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataParser getParser(String id, byte[] data) throws DataParserException {
    try {
      return new AvroMessageParser(getSettings().getContext(), schema, data, id, schemaInMessage);
    } catch (IOException e) {
      throw new DataParserException(Errors.DATA_PARSER_01, e.toString(), e);
    }
  }

  @Override
  public DataParser getParser(File file, String fileOffset)
    throws DataParserException {
    try {
      return new AvroDataFileParser(getSettings().getContext(), schema, file, fileOffset,
        getSettings().getOverRunLimit());
    } catch (IOException e) {
      throw new DataParserException(Errors.DATA_PARSER_01, e.toString(), e);
    }
  }
}
