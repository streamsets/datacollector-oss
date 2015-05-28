/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.json;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.JsonMode;
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

public class JsonDataParserFactory extends DataParserFactory {
  public static final Map<String, Object> CONFIGS = Collections.emptyMap();

  @SuppressWarnings("umchecked")
  public static final Set<Class<? extends Enum>> MODES =
      (Set<Class<? extends Enum>>) (Set) ImmutableSet.of(JsonMode.class);

  public JsonDataParserFactory(Settings settings) {
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
      return new JsonCharDataParser(getSettings().getContext(), id, reader, offset,
                                    getSettings().getMode(JsonMode.class).getFormat(), getSettings().getMaxRecordLen());
    } catch (IOException ex) {
      throw new DataParserException(Errors.JSON_PARSER_00, id, offset, ex.getMessage(), ex);
    }
  }

}
