/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.json;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.json.StreamingJsonParser;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.CharDataParserFactory;

import java.io.IOException;
import java.util.Map;

public class JsonCharDataParserFactory extends CharDataParserFactory {

  public static Map<String, Object> registerConfigs(Map<String, Object> configs) {
    return configs;
  }

  private final Stage.Context context;
  private final StreamingJsonParser.Mode mode;
  private final int maxObjectLen;

  public JsonCharDataParserFactory(Stage.Context context, int maxObjectLen, StreamingJsonParser.Mode mode,
      Map<String, Object> configs) {
    this.context = context;
    this.mode = mode;
    this.maxObjectLen = maxObjectLen;
  }

  @Override
  public DataParser getParser(String id, OverrunReader reader, long readerOffset) throws DataParserException {
    Utils.checkState(reader.getPos() == 0, Utils.formatL("reader must be in position '0', it is at '{}'",
                                                         reader.getPos()));
    try {
      return new JsonDataParser(context, id, reader, readerOffset, mode, maxObjectLen);
    } catch (IOException ex) {
      throw new DataParserException(Errors.JSON_PARSER_00, id, readerOffset, ex.getMessage(), ex);
    }
  }

}
