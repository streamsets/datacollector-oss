/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.text;

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
  public static final Map<String, Object> CONFIGS = Collections.emptyMap();
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
      return new TextCharDataParser(getSettings().getContext(), id, reader, offset, getSettings().getMaxRecordLen(),
        TEXT_FIELD_NAME, TRUNCATED_FIELD_NAME);
    } catch (IOException ex) {
      throw new DataParserException(Errors.TEXT_PARSER_00, id, offset, ex.getMessage(), ex);
    }
  }

}
