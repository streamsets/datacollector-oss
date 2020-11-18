/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.jsonparser;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.api.impl.TypeSupportConversionException;
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
      String value = null;
      try {
        value = field.getValueAsString();
      } catch (final TypeSupportConversionException ex) {
        throw new OnRecordErrorException(record, ex.errorCode, ex.params);
      }
      if (value == null) {
        throw new OnRecordErrorException(Errors.JSONP_01, record.getHeader().getSourceId(), fieldPathToParse);
      }
      try (OverrunReader reader = new OverrunReader(new StringReader(value), -1, false, removeCtrlChars)) {
        JsonCharDataParser parser = new JsonCharDataParser(getContext(), "", reader, 0, Mode.MULTIPLE_OBJECTS, -1);
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
