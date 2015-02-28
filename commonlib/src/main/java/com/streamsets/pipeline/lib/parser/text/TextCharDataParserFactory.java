/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.text;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.CharDataParserFactory;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.util.Map;

public class TextCharDataParserFactory extends CharDataParserFactory {
  static final String TEXT_FIELD_NAME = "text";
  static final String TRUNCATED_FIELD_NAME = "truncated";

  public static Map<String, Object> registerConfigs(Map<String, Object> configs) {
    return configs;
  }

  private final Stage.Context context;
  private final int maxObjectLen;

  public TextCharDataParserFactory(Stage.Context context, int maxObjectLen, Map<String, Object> configs) {
    this.context = context;
    this.maxObjectLen = maxObjectLen;
  }

  @Override
  public DataParser getParser(String id, OverrunReader reader, long readerOffset) throws DataParserException {
    Utils.checkState(reader.getPos() == 0, Utils.formatL("reader must be in position '0', it is at '{}'",
                                                         reader.getPos()));
    try {
      return new TextDataParser(context, id, reader, readerOffset, maxObjectLen, TEXT_FIELD_NAME, TRUNCATED_FIELD_NAME);
    } catch (IOException ex) {
      throw new DataParserException(Errors.TEXT_PARSER_00, id, readerOffset, ex.getMessage(), ex);
    }
  }

}
