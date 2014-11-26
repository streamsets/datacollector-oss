/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.lib.stage.source.spooldir.json;

import com.codahale.metrics.Counter;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.lib.io.CountingReader;
import com.streamsets.pipeline.lib.json.OverrunStreamingJsonParser;
import com.streamsets.pipeline.lib.json.StreamingJsonParser;
import com.streamsets.pipeline.lib.stage.source.spooldir.AbstractSpoolDirSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@StageDef(version = "1.0.0",
    label = "JSON files spool directory",
    description = "Consumes JSON files from a spool directory")
public class JsonSpoolDirSource extends AbstractSpoolDirSource {
  private final static Logger LOG = LoggerFactory.getLogger(JsonSpoolDirSource.class);

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      label = "JSON Content",
      description = "Indicates if the JSON files have a single JSON array object or multiple JSON objects, " +
                    "ARRAY_OBJECTS or MULTIPLE_OBJECTS",
      defaultValue = "ARRAY_OBJECTS")
  public String jsonContent;

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      label = "JSON Object Type",
      description = "Indicates the type of the JSON objects, MAP or ARRAY",
      defaultValue = "MAP")
  public String jsonObjectType;

  @ConfigDef(required = true,
      type = ConfigDef.Type.INTEGER,
      label = "Maximum JSON Object Length",
      description = "The maximum length for a JSON Object being converted to a record, if greater the full JSON " +
                    "object is discarded and processing continues with the next JSON object",
      defaultValue = "4096")
  public int maxJsonObjectLen;

  private StreamingJsonParser.Mode parserMode;
  private Class jsonType;
  private Counter jsonObjectsOverMaxLen;

  @Override
  protected void init() throws StageException {
    super.init();
    parserMode = StreamingJsonParser.Mode.valueOf(jsonContent);
    if (jsonObjectType.equals("MAP")) {
      jsonType = Map.class;
    } else if (jsonObjectType.equals("ARRAY")) {
      jsonType = List.class;
    } else {
      throw new StageException(null, jsonObjectType);
    }
    jsonObjectsOverMaxLen = getContext().createCounter("jsonObjectsOverMaxLen");
  }

  @Override
  protected long produce(File file, long offset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    String sourceFile = file.getName();
    try (CountingReader reader = new CountingReader(new FileReader(file))) {
      OverrunStreamingJsonParser parser = new OverrunStreamingJsonParser(reader, offset, parserMode, maxJsonObjectLen);
      return produce(sourceFile, offset, parser, maxBatchSize, batchMaker);
    } catch (IOException ex) {
      throw new StageException(null, ex.getMessage(), ex);
    }
  }

  protected long produce(String sourceFile, long offset, OverrunStreamingJsonParser parser, int maxBatchSize,
      BatchMaker batchMaker) throws IOException {
    for (int i = 0; i < maxBatchSize; i++) {
      try {
        Object json = parser.read(jsonType);
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

  protected Record createRecord(String sourceFile, long offset, Object json) {
    Record record = getContext().createRecord(Utils.format("file={} offset={}", sourceFile, offset));
    //TODO populate record with JSON stuff
    return record;
  }

}
