/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.jsonparser;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.json.StreamingJsonParser;
import com.streamsets.pipeline.lib.parser.json.JsonCharDataParser;

import java.io.IOException;
import java.io.StringReader;

public class JsonParserProcessor extends SingleLaneRecordProcessor {

  private final String fieldPathToParse;
  private final boolean removeCtrlChars;
  private final String parsedFieldPath;

  public JsonParserProcessor(String fieldPathToParse, boolean removeCtrlChars, String parsedFieldPath) {
    this.fieldPathToParse = fieldPathToParse;
    this.removeCtrlChars = removeCtrlChars;
    this.parsedFieldPath = parsedFieldPath;
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
      try (OverrunReader reader = new OverrunReader(new StringReader(value), -1, false, removeCtrlChars)) {
        JsonCharDataParser parser = new JsonCharDataParser(getContext(), "", reader, 0,
                                                           StreamingJsonParser.Mode.MULTIPLE_OBJECTS, -1);
        Field parsed = parser.parseAsField();
        if (parsed != null) {
          record.set(parsedFieldPath, parsed);
        }
      } catch (IOException ex) {
        throw new OnRecordErrorException(Errors.JSONP_03, record.getHeader().getSourceId(), fieldPathToParse,
                                         ex.toString(), ex);
      }
      batchMaker.addRecord(record);
    }
  }

}
