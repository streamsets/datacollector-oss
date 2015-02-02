/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.lib.json.StreamingJsonParser;

@GenerateResourceBundle
@RawSource(rawSourcePreviewer = KafkaRawSourcePreviewer.class, mimeType = "application/json")
@StageDef(version="0.0.1",
  label="Kafka Consumer",
  icon="kafka.png")
@ConfigGroups(value = HighLevelKafkaSource.KafkaSourceConfigGroups.class)
public class HighLevelKafkaSource extends HighLevelAbstractKafkaSource {

  @ConfigDef(required = true,
    type = ConfigDef.Type.MODEL,
    label = "JSON Content",
    description = "Indicates if the JSON files have a single JSON array object or multiple JSON objects",
    defaultValue = "ARRAY_OBJECTS",
    dependsOn = "payloadType",
    triggeredByValue = {"JSON"},
    group = "JSON_PROPERTIES")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = JsonFileModeChooserValues.class)
  public StreamingJsonParser.Mode jsonContent;

  @ConfigDef(required = true,
    type = ConfigDef.Type.INTEGER,
    label = "Maximum JSON Object Length",
    description = "The maximum length for a JSON Object being converted to a record, if greater the full JSON " +
      "object is discarded and processing continues with the next JSON object",
    defaultValue = "4096",
    dependsOn = "payloadType",
    triggeredByValue = {"JSON"},
    group = "JSON_PROPERTIES")
  public int maxJsonObjectLen;

  @ConfigDef(required = true,
    type = ConfigDef.Type.MODEL,
    label = "CSV Format",
    description = "The specific CSV format of the files",
    group = "CSV_PROPERTIES",
    dependsOn = "payloadType",
    triggeredByValue = {"CSV"},
    defaultValue = "CSV")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = CvsFileModeChooserValues.class)
  public CsvFileMode csvFileFormat;

  public enum KafkaSourceConfigGroups implements Label {
    JSON_PROPERTIES("JSON Data Properties"),
    CSV_PROPERTIES("CSV Data Properties"),
    XML_PROPERTIES("XML Data Properties"),
    LOG_PROPERTIES("Log Data Properties");

    private final String label;

    private KafkaSourceConfigGroups(String label) {
      this.label = label;
    }

    public String getLabel() {
      return this.label;
    }
  }

  private FieldCreator fieldCreator;

  @Override
  public void init() throws StageException {
    super.init();
    switch ((payloadType)) {
      case JSON:
        fieldCreator = new JsonFieldCreator(jsonContent, maxJsonObjectLen);
        break;
      case LOG:
        fieldCreator = new LogFieldCreator();
        break;
      case CSV:
        fieldCreator = new CsvFieldCreator(csvFileFormat);
        break;
      default :
    }
  }

  @Override
  protected Field createField(byte[] bytes) throws StageException {
    return fieldCreator.createField(bytes);
  }
}
