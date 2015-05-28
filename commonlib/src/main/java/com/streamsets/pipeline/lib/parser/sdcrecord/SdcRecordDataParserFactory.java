/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.sdcrecord;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class SdcRecordDataParserFactory extends DataParserFactory {
  public static final Map<String, Object> CONFIGS = Collections.emptyMap();
  public static final Set<Class<? extends Enum>> MODES = Collections.emptySet();

  public SdcRecordDataParserFactory(Settings settings) {
    super(settings);
  }

  @Override
  public DataParser getParser(String id, InputStream is, long offset) throws DataParserException {
    try {
      return new SdcRecordDataParser(getSettings().getContext(), is, offset,
        getSettings().getMaxRecordLen());
    } catch (IOException ex) {
      throw new DataParserException(Errors.SDC_RECORD_PARSER_00, id, offset, ex.getMessage(), ex);
    }
  }

  @Override
  public DataParser getParser(String id, Reader reader, long offset) throws DataParserException {
    throw new UnsupportedOperationException(Utils.format("{} does not support character based data", getClass().getName()));
  }

}
