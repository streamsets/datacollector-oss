/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.spooldir;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.csv.OverrunCsvParser;
import org.apache.commons.csv.CSVFormat;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CsvDataProducer implements DataProducer {
  private final Source.Context context;
  private final CSVFormat csvFormat;
  private final boolean hasHeaderLine;
  private final boolean convertToMap;
  private String[] headers;
  private List<Field> headerFields;
  private OverrunCsvParser parser;

  public CsvDataProducer(Source.Context context, CsvFileMode csvMode, boolean hasHeaderLine, boolean convertToMap) {
    this.context = context;
    this.csvFormat = (hasHeaderLine) ? csvMode.getFormat().withHeader() : csvMode.getFormat();
    this.hasHeaderLine = hasHeaderLine;
    this.convertToMap = convertToMap;
  }

  @Override
  public long produce(File file, long offset, int maxBatchSize, BatchMaker batchMaker)
      throws StageException, BadSpoolFileException {
    Reader reader = null;
    try {
      if (parser == null) {
        reader = new FileReader(file);
        parser = new OverrunCsvParser(reader, csvFormat, offset);
        reader = null;
        if (hasHeaderLine) {
          headers = parser.getHeaders();
          if (!convertToMap && headers != null) {
            // CSV is not been converted to a map (values keyed with the column name) but instead we keep the
            // values as an array and we set the headers as another array. We pre-compute the headers List<Field>
            // on per file basis, not to do it on every batch.
            headerFields = new ArrayList<>(headers.length);
            for (String header : headers) {
              headerFields.add(Field.create(header));
            }
          }
        }
      }
      offset = produce(file.getName(), offset, parser, maxBatchSize, batchMaker);
    } catch (IOException ex) {
      offset = -1;
      throw new BadSpoolFileException(file.getAbsolutePath(), (parser == null) ? 0 : parser.getReaderPosition(), ex);
    } finally {
      if (offset == -1) {
        if (parser != null) {
          parser.close();
          parser = null;
        }
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException ex) {
            //NOP
          }
        }
      }
    }
    return offset;
  }

  protected long produce(String sourceFile, long offset, OverrunCsvParser parser, int maxBatchSize,
      BatchMaker batchMaker) throws IOException, BadSpoolFileException {
      for (int i = 0; i < maxBatchSize; i++) {
      try {
        String[] columns = parser.read();
        if (columns != null) {
          Record record = createRecord(sourceFile, offset, i, columns);
          batchMaker.addRecord(record);
          offset = parser.getReaderPosition();
        } else {
          offset = -1;
          break;
        }
      } catch (IOException ex) {
        throw new BadSpoolFileException(sourceFile, parser.getReaderPosition(), ex);
      }
    }
    return offset;
  }

  protected Record createRecord(String sourceFile, long offset, int offsetIndex, String[] columns) throws IOException {
    Record record = context.createRecord(sourceFile + "::" + offset + "::" + offsetIndex);
    Map<String, Field> map = new LinkedHashMap<>();
    if (convertToMap) {
      for (int i = 0; i < columns.length; i++) {
        map.put(getColumnName(i), Field.create(columns[i]));
      }
    } else {
      if (hasHeaderLine) {
        map.put("headers", Field.create(headerFields));
      }
      List<Field> values = new ArrayList<>(columns.length);
      for (String column : columns) {
        values.add(Field.create(column));
      }
      map.put("values", Field.create(values));
    }
    record.set(Field.create(map));
    return record;
  }

  private String getColumnName(int pos) {
    String name = null;
    if (hasHeaderLine) {
      name = (pos >= headers.length) ? null : headers[pos];
    }
    if (name == null) {
      name = String.format("col_%04d", pos);
    }
    return name;
  }

}
