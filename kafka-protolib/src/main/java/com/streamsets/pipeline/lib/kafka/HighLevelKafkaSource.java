/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.lib.json.StreamingJsonParser;

import java.util.List;

@GenerateResourceBundle
@StageDef(
    version = "1.0.0",
    label = "Kafka Consumer",
    description = "Reads messages from Kafka brokers. Message data can be: TEXT, CSV, TSV, XML or JSON",
    icon = "kafka.png"
)
@RawSource(rawSourcePreviewer = KafkaRawSourcePreviewer.class, mimeType = "application/json")
@ConfigGroups(value = HighLevelKafkaSource.Groups.class)
public class HighLevelKafkaSource extends HighLevelAbstractKafkaSource {

  public enum Groups implements Label {
    JSON("JSON Data"),
    CSV("CSV Data")

    ;

    private final String label;

    private Groups(String label) {
      this.label = label;
    }

    public String getLabel() {
      return this.label;
    }
  }

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "MULTIPLE_OBJECTS",
      label = "JSON Content",
      description = "Indicates if the JSON files have a single JSON array object or multiple JSON objects",
      displayPosition = 100,
      group = "JSON",
      dependsOn = "consumerPayloadType",
      triggeredByValue = "JSON"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = JsonEventModeChooserValues.class)
  public StreamingJsonParser.Mode jsonContent;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "false",
    label = "Produce Single Record",
    description = "Indicates if multiple json objects must be accommodated in a single record.",
    displayPosition = 103,
    group = "JSON",
    dependsOn = "jsonContent",
    triggeredByValue = "MULTIPLE_OBJECTS"
  )
  public boolean produceSingleRecord;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.INTEGER,
      defaultValue = "4096",
      label = "Maximum JSON Object Length",
      description = "The maximum length for a JSON Object being converted to a record, if greater the full JSON " +
                    "object is discarded and processing continues with the next JSON object",
      displayPosition = 110,
      group = "JSON",
      dependsOn = "consumerPayloadType",
      triggeredByValue = "JSON"
  )
  public int maxJsonObjectLen;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "CSV",
      label = "CSV Format",
      description = "The specific CSV format of the files",
      displayPosition = 200,
      group = "CSV",
      dependsOn = "consumerPayloadType",
      triggeredByValue = "CSV"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = CvsFileModeChooserValues.class)
  public CsvFileMode csvFileFormat;

  private RecordCreator recordCreator;

  @Override
  public void init() throws StageException {
    super.init();
    switch ((consumerPayloadType)) {
      case JSON:
        recordCreator = new JsonRecordCreator(getContext(), jsonContent, maxJsonObjectLen, produceSingleRecord, topic);
        break;
      case LOG:
        recordCreator = new LogRecordCreator(getContext(), topic);
        break;
      case CSV:
        recordCreator = new CsvRecordCreator(getContext(), csvFileFormat, topic);
        break;
      case XML:
        recordCreator = new XmlRecordCreator(getContext(), topic);
        break;
      default :
    }
  }

  @Override
  protected List<Record> createRecords(MessageAndOffset message , int currentRecordCount) throws StageException {
    return recordCreator.createRecords(message, currentRecordCount);
  }
}
