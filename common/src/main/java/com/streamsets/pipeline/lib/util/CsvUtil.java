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
package com.streamsets.pipeline.lib.util;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CsvUtil {

  private CsvUtil() {}

  public static List<String> fieldToCsv(Field field) throws IOException {
    Map<String, Field> map  = field.getValueAsMap();
    List<String> values = new ArrayList<>();
    for(Field f : map.get("values").getValueAsList()) {
      values.add(getValueAsString(f));
    }
    return values;
  }

  private static String getValueAsString(Field field) throws IOException {
    if(field.getType()== Field.Type.BOOLEAN) {
      return String.valueOf(field.getValueAsBoolean());
    } else if(field.getType()== Field.Type.BYTE) {
      return String.valueOf(field.getValueAsByte());
    } else if(field.getType()== Field.Type.BYTE_ARRAY) {
      return String.valueOf(field.getValueAsByteArray());
    } else if(field.getType()== Field.Type.CHAR) {
      return String.valueOf(field.getValueAsChar());
    } else if(field.getType()== Field.Type.DATE) {
      return String.valueOf(field.getValueAsDate());
    } else if(field.getType()== Field.Type.DATETIME) {
      return String.valueOf(field.getValueAsDatetime());
    } else if(field.getType()== Field.Type.DECIMAL) {
      return String.valueOf(field.getValueAsDecimal());
    } else if(field.getType()== Field.Type.DOUBLE) {
      return String.valueOf(field.getValueAsDouble());
    } else if(field.getType()== Field.Type.FLOAT) {
      return String.valueOf(field.getValueAsFloat());
    } else if(field.getType()== Field.Type.INTEGER) {
      return String.valueOf(field.getValueAsInteger());
    } else if(field.getType()== Field.Type.LONG) {
      return String.valueOf(field.getValueAsLong());
    } else if(field.getType()== Field.Type.SHORT) {
      return String.valueOf(field.getValueAsShort());
    } else if(field.getType()== Field.Type.STRING) {
      return String.valueOf(field.getValueAsString());
    } else {
      throw new IOException(Utils.format("Not recognized type '{}', value '{}'", field.getType(), field.getValue()));
    }
  }

  public static String csvRecordToString(Record r, CSVFormat csvFormat) throws IOException {
    StringWriter stringWriter = new StringWriter();
    CSVPrinter csvPrinter = new CSVPrinter(stringWriter, csvFormat);
    csvPrinter.printRecord(CsvUtil.fieldToCsv(r.get()));
    csvPrinter.flush();
    csvPrinter.close();
    return stringWriter.toString();
  }
}
