/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
      throw new DataParserException(Errors.DATA_PARSER_01, e.getMessage(), e);
    }
  }

  public DataParser getParser(File file, String fileOffset)
    throws DataParserException {
    try {
      return new AvroDataFileParser(getSettings().getContext(), schema, file, fileOffset,
        getSettings().getOverRunLimit());
    } catch (IOException e) {
      throw new DataParserException(Errors.DATA_PARSER_01, e.getMessage(), e);
    }
  }
}
