/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.log;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.CharDataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;

import java.io.IOException;
import java.util.Map;

public class LogCharDataParserFactory extends CharDataParserFactory {

  static final String KEY_PREFIX = "log.";
  public static final String RETAIN_ORIGINAL_TEXT_KEY = KEY_PREFIX + "retain.original.text";
  static final boolean RETAIN_ORIGINAL_TEXT_DEFAULT = false;

  public static Map<String, Object> registerConfigs(Map<String, Object> configs) {
    configs.put(RETAIN_ORIGINAL_TEXT_KEY, RETAIN_ORIGINAL_TEXT_DEFAULT);
    return configs;
  }

  private final Stage.Context context;
  private final int maxObjectLen;
  private final LogMode logMode;
  private final boolean retainOriginalText;

  public LogCharDataParserFactory(Stage.Context context, int maxObjectLen, LogMode logMode,
                                  Map<String, Object> configs) {
    this.context = context;
    this.maxObjectLen = maxObjectLen;
    this.logMode = logMode;
    this.retainOriginalText = (boolean) configs.get(RETAIN_ORIGINAL_TEXT_KEY);
  }

  @Override
  public DataParser getParser(String id, OverrunReader reader, long readerOffset) throws DataParserException {
    Utils.checkState(reader.getPos() == 0, Utils.formatL("reader must be in position '0', it is at '{}'",
      reader.getPos()));
    try {
      switch (logMode) {
        case COMMON_LOG_FORMAT:
          return new CommonLogFormatParser(context, id, reader, readerOffset, maxObjectLen, retainOriginalText);
        default :
          return null;
      }
    } catch (IOException ex) {
      throw new DataParserException(Errors.LOG_PARSER_00, id, readerOffset, ex.getMessage(), ex);
    }
  }


}
