/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.sdcrecord;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.CharDataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;

import java.io.IOException;
import java.util.Map;

public class JsonSdcRecordCharDataParserFactory extends CharDataParserFactory {

  public static Map<String, Object> registerConfigs(Map<String, Object> configs) {
    return configs;
  }

  private final Stage.Context context;
  private final int maxObjectLength;

  public JsonSdcRecordCharDataParserFactory(Stage.Context context, int maxObjectLength, Map<String, Object> configs) {
    this.context = context;
    this.maxObjectLength = maxObjectLength;
  }

  @Override
  public DataParser getParser(String id, OverrunReader reader, long readerOffset) throws DataParserException {
    Utils.checkState(reader.getPos() == 0, Utils.formatL("reader must be in position '0', it is at '{}'",
                                                         reader.getPos()));
    try {
      return new JsonSdcRecordDataParser(context, reader, readerOffset, maxObjectLength);
    } catch (IOException ex) {
      throw new DataParserException(Errors.SDC_RECORD_PARSER_00, id, readerOffset, ex.getMessage(), ex);
    }
  }

}
