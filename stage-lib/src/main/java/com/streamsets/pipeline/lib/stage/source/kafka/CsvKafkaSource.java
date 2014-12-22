/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.lib.csv.OverrunCsvParser;
import com.streamsets.pipeline.lib.io.CountingReader;
import com.streamsets.pipeline.lib.stage.source.spooldir.csv.CvsFileModeChooserValues;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@GenerateResourceBundle
@RawSource(rawSourcePreviewer = KafkaRawSourcePreviewer.class, mimeType = "text/csv")
@StageDef(version="0.0.1",
  label="CSV Kafka Source")
public class CsvKafkaSource extends AbstractKafkaSource {

  @ConfigDef(required = true,
    type = ConfigDef.Type.MODEL,
    label = "CSV Format",
    description = "The specific CSV format of the files",
    defaultValue = "DEFAULT")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = CvsFileModeChooserValues.class)
  public String csvFileFormat;

  @Override
  protected void populateRecordFromBytes(Record record, byte[] bytes) throws StageException {
    try (CountingReader reader =
           new CountingReader(new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bytes))))) {
      OverrunCsvParser parser = new OverrunCsvParser(reader, CvsFileModeChooserValues.getCSVFormat(csvFileFormat));
      String[] columns = parser.read();
      Map<String, Field> map = new LinkedHashMap<>();
      List<Field> values = new ArrayList<>(columns.length);
      for (String column : columns) {
        values.add(Field.create(column));
      }
      map.put("values", Field.create(values));
      record.set(Field.create(map));
    }catch (Exception e) {
      throw new StageException(null, e.getMessage(), e);
    }
  }
}
