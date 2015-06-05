/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.delimited;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.lib.csv.OverrunCsvParser;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import org.apache.commons.csv.CSVFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DelimitedCharDataParser implements DataParser {
  private final Stage.Context context;
  private final String readerId;
  private final OverrunCsvParser parser;
  private List<Field> headers;
  private boolean eof;

  public DelimitedCharDataParser(Stage.Context context, String readerId, OverrunReader reader, long readerOffset,
                                 CSVFormat format, CsvHeader header, int maxObjectLen) throws IOException {
    this.context = context;
    this.readerId = readerId;
    switch (header) {
      case WITH_HEADER:
        format = format.withHeader((String[])null).withSkipHeaderRecord(true);
        break;
      case IGNORE_HEADER:
        format = format.withHeader((String[])null).withSkipHeaderRecord(true);
        break;
      case NO_HEADER:
        format = format.withHeader((String[])null).withSkipHeaderRecord(false);
        break;
      default:
        throw new RuntimeException("It should not happen");
    }
    parser = new OverrunCsvParser(reader, format, readerOffset, maxObjectLen);
    String[] hs = parser.getHeaders();
    if (header != CsvHeader.IGNORE_HEADER && hs != null) {
      headers = new ArrayList<>();
      for (String h : hs) {
        headers.add(Field.create(h));
      }
    }
  }

  @Override
  public Record parse() throws IOException, DataParserException {
    Record record = null;
    long offset = parser.getReaderPosition();
    String[] columns = parser.read();
    if (columns != null) {
      record = createRecord(offset, columns);
    } else {
      eof = true;
    }
    return record;
  }

  protected Record createRecord(long offset, String[] columns) throws DataParserException {
    Record record = context.createRecord(readerId + "::" + offset);

    List<Field> row = new ArrayList<>();
    for (int i = 0; i < columns.length; i++) {
      Map<String, Field> cell = new HashMap<>();
      Field header = (headers != null) ? headers.get(i) : null;
      if (header != null) {
        cell.put("header", header);
      }
      Field value = Field.create(columns[i]);
      cell.put("value", value);
      row.add(Field.create(cell));
    }
    record.set(Field.create(row));
    return record;
  }

  @Override
  public String getOffset() {
    return (eof) ? String.valueOf(-1) : String.valueOf(parser.getReaderPosition());
  }

  @Override
  public void close() throws IOException {
    parser.close();
  }

}
