/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.binary;

import com.streamsets.pipeline.lib.parser.*;
import java.io.InputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class BinaryDataParserFactory extends DataParserFactory {
  public static final Map<String, Object> CONFIGS = Collections.emptyMap();
  public static final Set<Class<? extends Enum>> MODES = Collections.emptySet();

  public BinaryDataParserFactory(Settings settings) {
    super(settings);
  }

  @Override
  public DataParser getParser(String id, InputStream is, long offset) throws DataParserException {
    return new BinaryDataParser(getSettings().getContext(), is, id, getSettings().getMaxRecordLen());
  }

  @Override
  public DataParser getParser(String id, Reader reader, long offset) throws DataParserException {
    throw new UnsupportedOperationException();
  }

}
