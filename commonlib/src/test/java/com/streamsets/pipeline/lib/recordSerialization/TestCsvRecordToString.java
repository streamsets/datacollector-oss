/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.recordSerialization;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.recordserialization.CsvRecordToString;
import com.streamsets.pipeline.lib.recordserialization.RecordToString;
import org.apache.commons.csv.CSVFormat;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestCsvRecordToString {

  @Test
  public void testCsvRecordToString() throws IOException, StageException {
    RecordToString recordToString = new CsvRecordToString(CSVFormat.DEFAULT);
    Map<String, String> fieldPathToName = new LinkedHashMap<>();
    fieldPathToName.put("/values[0]", "year");
    fieldPathToName.put("/values[10]", "name");
    fieldPathToName.put("/values[2]", "place");
    recordToString.setFieldPathToNameMapping(fieldPathToName);
    String result = recordToString.toString(createCsvRecord());
    Assert.assertEquals("2010,,PHI\r\n", result);
  }

  @Test
  public void testCsvRecordToStringByteArrayField() throws IOException, StageException {
    RecordToString recordToString = new CsvRecordToString(CSVFormat.DEFAULT);
    Map<String, String> fieldPathToName = new LinkedHashMap<>();
    fieldPathToName.put("/id", "ID");
    recordToString.setFieldPathToNameMapping(fieldPathToName);
    String result = recordToString.toString(createRecordWithByteArrayField(
      Field.create(Field.Type.BYTE_ARRAY, "Streamsets Inc".getBytes())));
    Assert.assertEquals("U3RyZWFtc2V0cyBJbmM=\r\n", result);
  }

  @Test
  public void testCsvRecordToStringByteArrayFieldNull() throws IOException, StageException {
    RecordToString recordToString = new CsvRecordToString(CSVFormat.DEFAULT);
    Map<String, String> fieldPathToName = new LinkedHashMap<>();
    fieldPathToName.put("/id", "ID");
    recordToString.setFieldPathToNameMapping(fieldPathToName);
    String result = recordToString.toString(createRecordWithByteArrayField(
      Field.create(Field.Type.BYTE_ARRAY, null)));
    Assert.assertEquals("\"\"\r\n", result);
  }

  private static Record createCsvRecord() throws IOException {
    String line;
    BufferedReader bufferedReader = new BufferedReader(new FileReader(TestCsvRecordToString.class.getClassLoader()
      .getResource("csvData.csv").getFile()));
    Map<String, Field> map = new LinkedHashMap<>();
    while ((line = bufferedReader.readLine()) != null) {
      String columns[] = line.split(",");
      List<Field> values = new ArrayList<>(columns.length);
      for (String column : columns) {
        values.add(Field.create(column));
      }
      map.put("values", Field.create(values));
      break;
    }
    Record record = Mockito.mock(Record.class);
    Mockito.when(record.get("/values[0]")).thenReturn(map.get("values").getValueAsList().get(0));
    Mockito.when(record.get("/values[2]")).thenReturn(map.get("values").getValueAsList().get(2));
    Mockito.when(record.get("/values[10]")).thenReturn(null);
    return record;
  }

  private static Record createRecordWithByteArrayField(Field f) throws IOException {
    Record record = Mockito.mock(Record.class);
    Mockito.when(record.get("/id")).thenReturn(f);
    return record;
  }
}
