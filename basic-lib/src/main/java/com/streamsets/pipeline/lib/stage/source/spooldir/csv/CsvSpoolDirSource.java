/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.spooldir.csv;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.FileRawSourcePreviewer;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.csv.OverrunCsvParser;
import com.streamsets.pipeline.lib.stage.source.spooldir.AbstractSpoolDirSource;
import com.streamsets.pipeline.lib.stage.source.spooldir.BadSpoolFileException;
import org.apache.commons.csv.CSVFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@GenerateResourceBundle
@RawSource(rawSourcePreviewer = FileRawSourcePreviewer.class, mimeType = "text/csv")
@StageDef(version = "1.0.0",
    label = "CSV files spool directory",
    description = "Consumes CSV files from a spool directory",
    icon = "csv.png")
public class CsvSpoolDirSource extends AbstractSpoolDirSource {
  private final static Logger LOG = LoggerFactory.getLogger(CsvSpoolDirSource.class);

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      label = "CSV Format",
      description = "The specific CSV format of the files",
      defaultValue = "DEFAULT")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = CvsFileModeChooserValues.class)
  public String csvFileFormat;


  @ConfigDef(required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Header Line",
      description = "Indicates if the CSV files start with a header line",
      defaultValue = "TRUE")
  public boolean hasHeaderLine;

  @ConfigDef(required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Convert to Map",
      description = "Converts CVS data array to a Map using the headers as keys",
      defaultValue = "TRUE")
  public boolean convertToMap;

  private CSVFormat csvFormat;
  private File previousFile;
  private String[] headers;
  private List<Field> headerFields;

  @Override
  protected void init() throws StageException {
    super.init();
    csvFormat = CvsFileModeChooserValues.getCSVFormat(csvFileFormat);
    if (hasHeaderLine) {
      csvFormat = csvFormat.withHeader();
    }
  }

  @Override
  protected long produce(File file, long offset, int maxBatchSize, BatchMaker batchMaker)
      throws StageException, BadSpoolFileException {
    OverrunCsvParser parser = null;
    if (hasHeaderLine) {
      if (previousFile == null || !previousFile.equals(file)) {
        // first file to process or new file, if first to process we don't have the headers, if file is different
        // from the previous produce() call we need to re-read the headers in case they are different.
        try (Reader reader = new FileReader(file)) {
          parser = new OverrunCsvParser(reader, csvFormat);
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
          offset = parser.getReaderPosition();
          previousFile = file;
        } catch (IOException ex) {
          throw new BadSpoolFileException(file.getAbsolutePath(), (parser == null) ? 0 : parser.getReaderPosition(),
                                          ex);
        }
      }
    }
    try (Reader reader = new FileReader(file)) {
      parser = new OverrunCsvParser(reader, csvFormat, offset);
      return produce(file.getName(), offset, parser, maxBatchSize, batchMaker);
    } catch (IOException ex) {
      throw new BadSpoolFileException(file.getAbsolutePath(), (parser == null) ? 0 : parser.getReaderPosition(), ex);
    }
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
    Record record = getContext().createRecord(Utils.format("file={} offset={} idx={}", sourceFile, offset,
                                                           offsetIndex));
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
