/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.xml;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.CharDataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class XmlCharDataParserFactory extends CharDataParserFactory {
  static final String KEY_PREFIX = "xml.";
  public static final String RECORD_ELEMENT_KEY = KEY_PREFIX + "record.element";
  static final String RECORD_ELEMENT_DEFAULT = "";

  public static final Map<String, Object> CONFIGS = ImmutableMap.of(RECORD_ELEMENT_KEY, (Object) RECORD_ELEMENT_DEFAULT);
  public static final Set<Class<? extends Enum>> MODES = Collections.emptySet();

  public XmlCharDataParserFactory(Settings settings) {
    super(settings);
  }

  @Override
  public DataParser getParser(String id, OverrunReader reader, long readerOffset) throws DataParserException {
    Utils.checkState(reader.getPos() == 0, Utils.formatL("reader must be in position '0', it is at '{}'",
                                                         reader.getPos()));
    try {
      return new XmlDataParser(getSettings().getContext(), id, reader, readerOffset,
                               (String) getSettings().getConfig(RECORD_ELEMENT_KEY), getSettings().getMaxRecordLen());
    } catch (IOException ex) {
      throw new DataParserException(Errors.XML_PARSER_00, id, readerOffset, ex.getMessage(), ex);
    }
  }

}
