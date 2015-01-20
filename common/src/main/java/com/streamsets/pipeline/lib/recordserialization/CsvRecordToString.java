/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.recordserialization;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.util.CommonError;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CsvRecordToString implements RecordToString {

  private final CSVFormat csvFormat;

  //Kafka target configuration asks for column name to fieldPath map
  private Map<String, String> fieldPathToNameMap;

  public CsvRecordToString(CSVFormat csvFormat) {
    this.csvFormat = csvFormat;
  }

  @Override
  public void setFieldPathToNameMapping(Map<String, String> fieldPathToNameMap) {
    this.fieldPathToNameMap = fieldPathToNameMap;
  }

  @Override
  public String toString(Record record) throws StageException {
    if(fieldPathToNameMap == null || fieldPathToNameMap.isEmpty()) {
      throw new StageException(CommonError.CMN_0102);
    }
    try {
      StringWriter stringWriter = new StringWriter();
      CSVPrinter csvPrinter = new CSVPrinter(stringWriter, csvFormat);
      csvPrinter.printRecord(recordToCsv(record));
      csvPrinter.flush();
      csvPrinter.close();
      return stringWriter.toString();
    } catch (IOException e) {
      throw new StageException(CommonError.CMN_0101, e.getMessage(), e);
    }
  }

  private List<String> recordToCsv(Record record) throws StageException {
    List<String> values = new ArrayList<>();
    for(Map.Entry<String, String> entry : fieldPathToNameMap.entrySet()) {
      Field field = record.get(entry.getKey());
      if(field != null) {
        values.add(getValueAsString(record, field));
      } else {
        values.add("");
      }
    }
    return values;
  }

  private String getValueAsString(Record record, Field field) throws StageException {
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
      throw new StageException(CommonError.CMN_0100, field.getType(),field.getValue(),
        record.getHeader().getSourceId());
    }
  }

}
