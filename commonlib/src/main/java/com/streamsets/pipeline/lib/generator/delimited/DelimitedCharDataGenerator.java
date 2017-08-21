/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

public class DelimitedCharDataGenerator implements DataGenerator {
  private final CSVFormat format;
  private final CsvHeader header;
  private final String headerKey;
  private final String valueKey;
  private final CSVPrinter printer;
  private boolean firstRecord;
  private boolean closed;
  /**
   * Optional replacement string that will be used to substitute all new line
   * characters (\n and \r) when serializing data to delimited format.
   */
  private String replaceNewLines;

  public DelimitedCharDataGenerator(Writer writer, CSVFormat format, CsvHeader header, String headerKey, String valueKey,
                                    String replaceNewLines)
      throws IOException {
    format = format.withHeader((String[])null);
    this.format = format;
    this.headerKey = headerKey;
    this.valueKey = valueKey;
    printer = new CSVPrinter(writer, format);
    this.header = header;
    firstRecord = true;
    this.replaceNewLines = replaceNewLines;
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
        writeHeader(record, getHeaderKey());
      }
      firstRecord = false;
    }
    writeLine(record, getValueKey());
  }

  private void writeHeader(Record record, String key) throws DataGeneratorException, IOException {
    Field field = record.get();

    if (field.getType() == Field.Type.LIST) {
      writeLine(record, key);
    } else if (field.getType() == Field.Type.LIST_MAP) {
      LinkedHashMap<String, Field> columns = field.getValueAsListMap();
      Set<String> values = columns.keySet(); // Set is backed by LinkedHashMap, so order is maintained
      printer.printRecord(values);
    } else {
      throw new DataGeneratorException(Errors.DELIMITED_GENERATOR_00, record.getHeader().getSourceId(), field.getType());
    }


  }

  private void writeLine(Record record, String key) throws IOException, DataGeneratorException{
    Field field = record.get();

    if (field.getType() != Field.Type.LIST && field.getType() != Field.Type.LIST_MAP) {
      throw new DataGeneratorException(Errors.DELIMITED_GENERATOR_00, record.getHeader().getSourceId(), field.getType());
    }

    List<Field> columns = field.getValueAsList();
    List<String> values = new ArrayList<>(columns.size());
    boolean isListMap = field.getType() == Field.Type.LIST_MAP;
    for (int i = 0; i< columns.size(); i++) {
      Field column = columns.get(i);
      String value;
      if (!isListMap) {
        try {
          value = column.getValueAsMap().get(key).getValueAsString();
        } catch (Exception ex) {
          throw new DataGeneratorException(Errors.DELIMITED_GENERATOR_01, record.getHeader().getSourceId(), i,
              column.getType());
        }
      } else {
        value = column.getValueAsString();
      }
      if (value != null) {
        if (replaceNewLines != null) {
          if (value.contains("\n")) {
            value = value.replace("\n", replaceNewLines);
          }
          if (value.contains("\r")) {
            value = value.replace("\r", replaceNewLines);
          }
        }
      } else {
        value = "";
      }
      values.add(value);
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
