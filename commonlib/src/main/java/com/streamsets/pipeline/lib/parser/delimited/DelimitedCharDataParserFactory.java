/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.delimited;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.CharDataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import org.apache.commons.csv.CSVFormat;

import java.io.IOException;
import java.util.Map;

public class DelimitedCharDataParserFactory extends CharDataParserFactory {

  public static Map<String, Object> registerConfigs(Map<String, Object> configs) {
    return configs;
  }

  private final Stage.Context context;
  private final CSVFormat format;
  private final CsvHeader header;

  public DelimitedCharDataParserFactory(Stage.Context context, CSVFormat format, CsvHeader header,
      Map<String, Object> configs) {
    this.context = context;
    this.format = format;
    this.header = header;
  }

  @Override
  public DataParser getParser(String id, OverrunReader reader, long readerOffset) throws DataParserException {
    Utils.checkState(reader.getPos() == 0, Utils.formatL("reader must be in position '0', it is at '{}'",
                                                         reader.getPos()));
    try {
      return new DelimitedDataParser(context, id, reader, readerOffset, format, header);
    } catch (IOException ex) {
      throw new DataParserException(Errors.DELIMITED_PARSER_00, id, readerOffset, ex.getMessage(), ex);
    }
  }

}
