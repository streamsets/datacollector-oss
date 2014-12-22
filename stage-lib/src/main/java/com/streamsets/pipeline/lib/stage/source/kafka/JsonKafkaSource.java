/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.lib.io.CountingReader;
import com.streamsets.pipeline.lib.json.OverrunStreamingJsonParser;
import com.streamsets.pipeline.lib.json.StreamingJsonParser;
import com.streamsets.pipeline.lib.stage.source.spooldir.json.JsonFileModeChooserValues;
import com.streamsets.pipeline.lib.stage.source.util.JsonUtil;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;

@GenerateResourceBundle
@RawSource(rawSourcePreviewer = KafkaRawSourcePreviewer.class, mimeType = "application/json")
@StageDef(version="0.0.1",
  label="Json Kafka Source",
  icon="kafka.png")
public class JsonKafkaSource extends AbstractKafkaSource {

  @ConfigDef(required = true,
    type = ConfigDef.Type.MODEL,
    label = "JSON Content",
    description = "Indicates if the JSON files have a single JSON array object or multiple JSON objects",
    defaultValue = "ARRAY_OBJECTS")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = JsonFileModeChooserValues.class)
  public StreamingJsonParser.Mode jsonContent;

  @ConfigDef(required = true,
    type = ConfigDef.Type.INTEGER,
    label = "Maximum JSON Object Length",
    description = "The maximum length for a JSON Object being converted to a record, if greater the full JSON " +
      "object is discarded and processing continues with the next JSON object",
    defaultValue = "4096")
  public int maxJsonObjectLen;

  @Override
  protected void populateRecordFromBytes(Record record, byte[] bytes) throws StageException {
    try (CountingReader reader =
           new CountingReader(new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bytes))))) {
      OverrunStreamingJsonParser parser = new OverrunStreamingJsonParser(reader, jsonContent, maxJsonObjectLen);
      record.set(JsonUtil.jsonToField(parser.read()));
    } catch (Exception e) {
      throw new StageException(null, e.getMessage(), e);
    }
  }
}
