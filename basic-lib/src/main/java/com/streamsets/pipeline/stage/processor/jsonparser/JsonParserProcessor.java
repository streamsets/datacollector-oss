/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.jsonparser;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.lib.util.JsonLineToRecord;
import com.streamsets.pipeline.lib.util.ToRecord;
import com.streamsets.pipeline.lib.util.ToRecordException;

@GenerateResourceBundle
@StageDef(
    version="1.0.0",
    label="JSON Parser",
    description = "Parses a string field with JSON data",
    icon="jsonparser.svg"
)
@ConfigGroups(com.streamsets.pipeline.stage.processor.jsonparser.ConfigGroups.class)
public class JsonParserProcessor extends SingleLaneRecordProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Field to Parse",
      description = "String field that contains a JSON object",
      displayPosition = 10,
      group = "JSON"
  )
  public String fieldPathToParse;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "New Parsed Field",
      description="Name of the new field to set the parsed JSON data",
      displayPosition = 20,
      group = "JSON"
  )
  public String parsedFieldPath;

  private JsonLineToRecord parser;

  @Override
  protected void init() throws StageException {
    super.init();
    parser = new JsonLineToRecord();
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Field field = record.get(fieldPathToParse);
    if (field == null) {
      throw new OnRecordErrorException(Errors.JSONP_00, record.getHeader().getSourceId(), fieldPathToParse);
    } else {
      String value = field.getValueAsString();
      if (value == null) {
        throw new OnRecordErrorException(Errors.JSONP_01, record.getHeader().getSourceId(), fieldPathToParse);
      }
      try {
        Field parsed = parser.parse(value);
        record.set(parsedFieldPath, parsed);
      } catch (ToRecordException ex) {
        throw new OnRecordErrorException(Errors.JSONP_03, record.getHeader().getSourceId(), fieldPathToParse,
                                         ex.getMessage(), ex);
      }
      if (!record.has(parsedFieldPath)) {
        throw new OnRecordErrorException(Errors.JSONP_02, record.getHeader().getSourceId(), parsedFieldPath);
      }
      batchMaker.addRecord(record);
    }
  }

}
