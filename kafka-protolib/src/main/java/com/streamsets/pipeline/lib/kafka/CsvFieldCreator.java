/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.csv.OverrunCsvParser;
import com.streamsets.pipeline.lib.io.CountingReader;
import com.streamsets.pipeline.lib.util.KafkaStageLibError;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CsvFieldCreator implements FieldCreator {

  private final CsvFileMode csvFileMode;

  public CsvFieldCreator(CsvFileMode csvFileMode) {
    this.csvFileMode = csvFileMode;
  }

  @Override
  public Field createField(byte[] bytes) throws StageException {
    try (CountingReader reader =
           new CountingReader(new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bytes))))) {
      OverrunCsvParser parser = new OverrunCsvParser(reader, csvFileMode.getFormat());
      String[] columns = parser.read();
      Map<String, Field> map = new LinkedHashMap<>();
      List<Field> values = new ArrayList<>(columns.length);
      for (String column : columns) {
        values.add(Field.create(column));
      }
      map.put("values", Field.create(values));
      return Field.create(map);
    }catch (Exception e) {
      throw new StageException(KafkaStageLibError.KFK_0100, e.getMessage(), e);
    }
  }
}
