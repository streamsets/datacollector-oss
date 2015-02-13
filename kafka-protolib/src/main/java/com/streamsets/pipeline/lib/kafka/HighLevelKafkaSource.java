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
    description = "Reads data from Kafka",
    icon = "kafka.png"
)
@RawSource(rawSourcePreviewer = KafkaRawSourcePreviewer.class, mimeType = "application/json")
@ConfigGroups(value = HighLevelKafkaSource.Groups.class)
public class HighLevelKafkaSource extends HighLevelAbstractKafkaSource {

  public enum Groups implements Label {
    JSON("JSON"),
    CSV("Delimited")

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
      defaultValue = "ARRAY_OBJECTS",
      label = "JSON Content",
      description = "",
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
    description = "Generates a single record for multiple JSON objects",
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
      label = "Max Object Length (chars)",
      description = "Longer objects are skipped",
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
      label = "Delimiter Format Type",
      description = "",
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
      case SDC_RECORDS:
        recordCreator = new SDCRecordCreator(getContext());
        break;
      default :
    }
  }

  @Override
  protected List<Record> createRecords(MessageAndOffset message , int currentRecordCount) throws StageException {
    return recordCreator.createRecords(message, currentRecordCount);
  }
}
