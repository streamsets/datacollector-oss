/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator.delimited;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

public class DelimitedDataGenerator implements DataGenerator {
  private final CSVFormat format;
  private final CsvHeader header;
  private final String headerKey;
  private final String valueKey;
  private final CSVPrinter printer;
  private boolean firstRecord;
  private boolean closed;

  public DelimitedDataGenerator(Writer writer, CSVFormat format, CsvHeader header, String headerKey, String valueKey)
      throws IOException {
    format = format.withHeader((String[])null);
    this.format = format;
    this.headerKey = headerKey;
    this.valueKey = valueKey;
    printer = new CSVPrinter(writer, format);
    this.header = header;
    firstRecord = true;
  }

  //VisibleForTesting
  CSVFormat getFormat() {
    return format;
  }

  //VisibleForTesting
  CsvHeader getHeader() {
    return header;
  }

  //VisibleForTesting
  String getHeaderKey() {
    return headerKey;
  }

  //VisibleForTesting
  String getValueKey() {
    return valueKey;
  }

  @Override
  public void write(Record record) throws IOException, DataGeneratorException {
    if (closed) {
      throw new IOException("generator has been closed");
    }
    if (firstRecord) {
      if (getHeader() == CsvHeader.WITH_HEADER) {
        writeLine(record, getHeaderKey());
      }
      firstRecord = false;
    }
    writeLine(record, getValueKey());
  }

  private void writeLine(Record record, String key) throws IOException, DataGeneratorException{
    Field field = record.get();
    if (field.getType() != Field.Type.LIST) {
      throw new DataGeneratorException(Errors.DELIMITED_GENERATOR_00, record.getHeader().getSourceId(), field.getType());
    }
    List<Field> columns = field.getValueAsList();
    List<String> values = new ArrayList<>(columns.size());
    for (int i = 0; i< columns.size(); i++) {
      Field column = columns.get(i);
      try {
        values.add(column.getValueAsMap().get(key).getValueAsString());
      } catch (Exception ex) {
        throw new DataGeneratorException(Errors.DELIMITED_GENERATOR_01, record.getHeader().getSourceId(), i,
                                         column.getType());
      }
    }
    printer.printRecord(values);
  }

  @Override
  public void flush() throws IOException {
    if (closed) {
      throw new IOException("generator has been closed");
    }
    printer.flush();
  }

  @Override
  public void close() throws IOException {
    closed = true;
    printer.close();
  }
}
