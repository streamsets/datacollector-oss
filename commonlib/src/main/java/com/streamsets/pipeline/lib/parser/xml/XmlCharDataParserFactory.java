/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.xml;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.CharDataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;

import java.io.IOException;
import java.util.Map;

public class XmlCharDataParserFactory extends CharDataParserFactory {
  static final String KEY_PREFIX = "xml.";
  public static final String RECORD_ELEMENT_KEY = KEY_PREFIX + "record.element";
  static final String RECORD_ELEMENT_DEFAULT = "";

  public static Map<String, Object> registerConfigs(Map<String, Object> configs) {
    configs.put(RECORD_ELEMENT_KEY, RECORD_ELEMENT_DEFAULT);
    return configs;
  }

  private final Stage.Context context;
  private final String recordElement;
  private final int maxObjectLen;

  public XmlCharDataParserFactory(Stage.Context context, int maxObjectLen, Map<String, Object> configs) {
    this.context = context;
    this.recordElement = (String) configs.get(RECORD_ELEMENT_KEY);
    this.maxObjectLen = maxObjectLen;
  }

  @Override
  public DataParser getParser(String id, OverrunReader reader, long readerOffset) throws DataParserException {
    Utils.checkState(reader.getPos() == 0, Utils.formatL("reader must be in position '0', it is at '{}'",
                                                         reader.getPos()));
    try {
      return new XmlDataParser(context, id, reader, readerOffset, recordElement, maxObjectLen);
    } catch (IOException ex) {
      throw new DataParserException(Errors.XML_PARSER_00, id, readerOffset, ex.getMessage(), ex);
    }
  }

}
