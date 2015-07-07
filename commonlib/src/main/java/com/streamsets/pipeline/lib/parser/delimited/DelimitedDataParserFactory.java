/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.delimited;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import org.apache.commons.csv.CSVFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class DelimitedDataParserFactory extends DataParserFactory {
  public static final String DELIMITER_CONFIG = "delimiterChar";
  public static final String ESCAPE_CONFIG = "escapeChar";
  public static final String QUOTE_CONFIG = "quoteChar";

  public static final Map<String, Object> CONFIGS =
      ImmutableMap.<String, Object>of(DELIMITER_CONFIG, '|', ESCAPE_CONFIG, '\\', QUOTE_CONFIG, '"');
  public static final Set<Class<? extends Enum>> MODES =
      ImmutableSet.of((Class<? extends Enum>) CsvMode.class, CsvHeader.class);

  public DelimitedDataParserFactory(Settings settings) {
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
    CSVFormat csvFormat = getSettings().getMode(CsvMode.class).getFormat();
    if (getSettings().getMode(CsvMode.class) == CsvMode.CUSTOM) {
      csvFormat = CSVFormat.DEFAULT.withDelimiter((char)getSettings().getConfig(DELIMITER_CONFIG))
                                   .withEscape((char) getSettings().getConfig(ESCAPE_CONFIG))
                                   .withQuote((char)getSettings().getConfig(QUOTE_CONFIG));
    }
    try {
      return new DelimitedCharDataParser(getSettings().getContext(), id, reader, offset, csvFormat,
                                         getSettings().getMode(CsvHeader.class), getSettings().getMaxRecordLen());
    } catch (IOException ex) {
      throw new DataParserException(Errors.DELIMITED_PARSER_00, id, offset, ex.getMessage(), ex);
    }
  }

}
