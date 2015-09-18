/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import java.util.List;

public class DelimitedCharDataGenerator implements DataGenerator {
  private final CSVFormat format;
  private final CsvHeader header;
  private final String headerKey;
  private final String valueKey;
  private final CSVPrinter printer;
  private boolean firstRecord;
  private boolean closed;
  private boolean replaceNewLines;

  public DelimitedCharDataGenerator(Writer writer, CSVFormat format, CsvHeader header, String headerKey, String valueKey,
                                    boolean replaceNewLines)
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
        String value = column.getValueAsMap().get(key).getValueAsString();
        if (replaceNewLines) {
          if (value.contains("\n")) {
            value = value.replace('\n', ' ');
          }
          if (value.contains("\r")) {
            value = value.replace('\r', ' ');
          }
        }
        values.add(value);
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
