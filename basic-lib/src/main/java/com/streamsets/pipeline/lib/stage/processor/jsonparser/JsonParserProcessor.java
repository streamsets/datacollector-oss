/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.jsonparser;

import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.lib.util.JsonLineToRecord;
import com.streamsets.pipeline.lib.util.StageLibError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@GenerateResourceBundle
@StageDef(
    version="1.0.0",
    label="JSON Parser",
    description = "Parses a String field with JSON data into a Record",
    icon="jsonparser.svg")
public class JsonParserProcessor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(JsonParserProcessor.class);

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      label = "Field to Parse",
      defaultValue = "",
      description = "Record field path of the JSON string to parse")
  public String fieldPathToParse;

  @ConfigDef(label = "Parsed JSON Field",
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      description="Record field path to set the parsed JSON")
  public String parsedFieldPath;

  @ConfigDef(label = "To Error If Not Enough Splits",
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "DISCARD",
      description="What to do with the record if there is a problem parsing the specified field")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = OnRecordProcessingErrorChooserValues.class)
  public OnRecordProcessingError onRecordProcessingError;

  private JsonLineToRecord parser;

  @Override
  protected void init() throws StageException {
    super.init();
    parser = new JsonLineToRecord();
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    try {
      Field field = record.get(fieldPathToParse);
      if (field == null) {
        throw new StageException(StageLibError.LIB_0800, record.getHeader().getSourceId(), fieldPathToParse);
      } else {
        String value = field.getValueAsString();
        if (value == null) {
          throw new StageException(StageLibError.LIB_0801, record.getHeader().getSourceId(), fieldPathToParse);
        }
        Field parsed = parser.parse(value);
        record.set(parsedFieldPath, parsed);
        if (!record.has(parsedFieldPath)) {
          throw new StageException(StageLibError.LIB_0802, record.getHeader().getSourceId(), parsedFieldPath);
        }
        batchMaker.addRecord(record);
      }

    } catch (StageException ex) {
      switch (onRecordProcessingError) {
        case DISCARD:
          LOG.debug("Discarding record '{}', {}", record.getHeader().getSourceId(), ex.getMessage(), ex);
          break;
        case TO_ERROR:
          getContext().toError(record, ex);
          break;
        case STOP_PIPELINE:
          throw ex;
      }
    }
  }

}
