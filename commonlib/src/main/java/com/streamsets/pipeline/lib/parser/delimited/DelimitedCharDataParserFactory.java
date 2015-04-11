/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.delimited;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.CharDataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class DelimitedCharDataParserFactory extends CharDataParserFactory {
  public static final Map<String, Object> CONFIGS = Collections.emptyMap();
  public static final Set<Class<? extends Enum>> MODES =
      ImmutableSet.of((Class<? extends Enum>) CsvMode.class, CsvHeader.class);

  public DelimitedCharDataParserFactory(Settings settings) {
    super(settings);
  }

  @Override
  public DataParser getParser(String id, InputStream is, long offset) throws DataParserException {
    OverrunReader reader = createReader(is);
    Utils.checkState(reader.getPos() == 0, Utils.formatL("reader must be in position '0', it is at '{}'",
      reader.getPos()));
    try {
      return new DelimitedDataParser(getSettings().getContext(), id, reader, offset,
        getSettings().getMode(CsvMode.class).getFormat(),
        getSettings().getMode(CsvHeader.class),
        getSettings().getMaxRecordLen());
    } catch (IOException ex) {
      throw new DataParserException(Errors.DELIMITED_PARSER_00, id, offset, ex.getMessage(), ex);
    }
  }

}
