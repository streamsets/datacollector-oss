/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.lib.csv.OverrunCsvParser;
import com.streamsets.pipeline.lib.io.CountingReader;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CsvRecordCreator implements RecordCreator {

  private static final String DOT = ".";

  private final CsvMode csvFileMode;
  private final Source.Context context;
  private final String topic;

  public CsvRecordCreator(Source.Context context, CsvMode csvFileMode, String topic) {
    this.csvFileMode = csvFileMode;
    this.context = context;
    this.topic = topic;
  }

  @Override
  public List<Record> createRecords(MessageAndOffset message, int currentRecordCount) throws StageException {
    try (CountingReader reader =
           new CountingReader(new BufferedReader(new InputStreamReader(
             new ByteArrayInputStream(message.getPayload()))))) {
      OverrunCsvParser parser = new OverrunCsvParser(reader, csvFileMode.getFormat());
      String[] columns = parser.read();
      Map<String, Field> map = new LinkedHashMap<>();
      List<Field> values = new ArrayList<>(columns.length);
      for (String column : columns) {
        values.add(Field.create(column));
      }
      map.put("values", Field.create(values));
      Record record = RecordCreatorUtil.createRecord(context, topic, message.getPartition(), currentRecordCount++);
      record.set(Field.create(map));
      return ImmutableList.of(record);
    }catch (Exception e) {
      throw new StageException(Errors.KAFKA_00, e.getMessage(), e);
    }
  }
}
