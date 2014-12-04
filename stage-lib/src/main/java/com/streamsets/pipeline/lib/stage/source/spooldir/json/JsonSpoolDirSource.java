/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.spooldir.json;

import com.codahale.metrics.Counter;
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
import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.lib.io.CountingReader;
import com.streamsets.pipeline.lib.io.OverrunException;
import com.streamsets.pipeline.lib.json.OverrunStreamingJsonParser;
import com.streamsets.pipeline.lib.json.StreamingJsonParser;
import com.streamsets.pipeline.lib.stage.source.spooldir.AbstractSpoolDirSource;
import com.streamsets.pipeline.lib.stage.source.spooldir.BadSpoolFileException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@GenerateResourceBundle
@RawSource(rawSourcePreviewer = FileRawSourcePreviewer.class, mimeType = "application/json")
@StageDef(version = "1.0.0",
    label = "JSON files spool directory",
    description = "Consumes JSON files from a spool directory")
public class JsonSpoolDirSource extends AbstractSpoolDirSource {
  private final static Logger LOG = LoggerFactory.getLogger(JsonSpoolDirSource.class);

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      label = "JSON Content",
      description = "Indicates if the JSON files have a single JSON array object or multiple JSON objects",
      defaultValue = "ARRAY_OBJECTS")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = JsonFileModeChooserValues.class)
  public String jsonContent;

  @ConfigDef(required = true,
      type = ConfigDef.Type.INTEGER,
      label = "Maximum JSON Object Length",
      description = "The maximum length for a JSON Object being converted to a record, if greater the full JSON " +
                    "object is discarded and processing continues with the next JSON object",
      defaultValue = "4096")
  public int maxJsonObjectLen;

  private StreamingJsonParser.Mode parserMode;
  private Counter jsonObjectsOverMaxLen;

  @Override
  protected void init() throws StageException {
    super.init();
    parserMode = StreamingJsonParser.Mode.valueOf(jsonContent);
    jsonObjectsOverMaxLen = getContext().createCounter("jsonObjectsOverMaxLen");
  }

  @Override
  protected long produce(File file, long offset, int maxBatchSize, BatchMaker batchMaker)
      throws StageException, BadSpoolFileException {
    String sourceFile = file.getName();
    OverrunStreamingJsonParser parser = null;
    try (CountingReader reader = new CountingReader(new FileReader(file))) {
      parser = new OverrunStreamingJsonParser(reader, offset, parserMode, maxJsonObjectLen);
      return produce(sourceFile, offset, parser, maxBatchSize, batchMaker);
    } catch (OverrunException ex) {
      throw new BadSpoolFileException(file.getAbsolutePath(), ex.getStreamOffset(), ex);
    } catch (IOException ex) {
      long exOffset = (parser != null) ? parser.getReaderPosition() : -1;
      throw new BadSpoolFileException(file.getAbsolutePath(), exOffset, ex);
    }
  }

  protected long produce(String sourceFile, long offset, OverrunStreamingJsonParser parser, int maxBatchSize,
      BatchMaker batchMaker) throws IOException {
    for (int i = 0; i < maxBatchSize; i++) {
      try {
        Object json = parser.read();
        if (json != null) {
          Record record = createRecord(sourceFile, offset, json);
          batchMaker.addRecord(record);
          offset = parser.getReaderPosition();
        } else {
          offset = -1;
          break;
        }
      } catch (OverrunStreamingJsonParser.JsonObjectLengthException ex) {
        jsonObjectsOverMaxLen.inc();
        LOG.warn("Discarding Json Object '{}', it exceeds maximum length '{}', file '{}', object starts at offset '{}'",
                 ex.getJsonSnippet(), maxJsonObjectLen, sourceFile, offset);
      }
    }
    return offset;
  }

  protected Record createRecord(String sourceFile, long offset, Object json) throws IOException {
    Record record = getContext().createRecord(Utils.format("file={} offset={}", sourceFile, offset));
    record.set(jsonToField(json));
    return record;
  }

  @SuppressWarnings("unchecked")
  protected Field jsonToField(Object json) throws IOException {
    Field field;
    if (json == null) {
      field = Field.create(Field.Type.STRING, null);
    } else if (json instanceof List) {
      List jsonList = (List) json;
      List<Field> list = new ArrayList<>(jsonList.size());
      for (Object element : jsonList) {
        list.add(jsonToField(element));
      }
      field = Field.create(list);
    } else if (json instanceof Map) {
      Map<String, Object> jsonMap = (Map<String, Object>) json;
      Map<String, Field> map = new LinkedHashMap<>();
      for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {
        map.put(entry.getKey(), jsonToField(entry.getValue()));
      }
      field = Field.create(map);
    } else if (json instanceof String) {
      field = Field.create((String) json);
    } else if (json instanceof Boolean) {
      field = Field.create((Boolean) json);
    } else if (json instanceof Character) {
      field = Field.create((Character) json);
    } else if (json instanceof Byte) {
      field = Field.create((Byte) json);
    } else if (json instanceof Short) {
      field = Field.create((Short) json);
    } else if (json instanceof Integer) {
      field = Field.create((Integer) json);
    } else if (json instanceof Long) {
      field = Field.create((Long) json);
    } else if (json instanceof Float) {
      field = Field.create((Float) json);
    } else if (json instanceof Double) {
      field = Field.create((Double) json);
    } else if (json instanceof byte[]) {
      field = Field.create((byte[]) json);
    } else if (json instanceof Date) {
      field = Field.createDate((Date) json);
    } else if (json instanceof BigDecimal) {
      field = Field.create((BigDecimal) json);
    } else {
      throw new IOException(Utils.format("Not recognized type '{}', value '{}'", json.getClass(), json));
    }
    return field;
  }
}
