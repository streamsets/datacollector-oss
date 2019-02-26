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
package com.streamsets.pipeline.stage.processor.logparser;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.service.dataformats.DataParser;
import com.streamsets.pipeline.api.service.dataformats.DataParserException;
import com.streamsets.pipeline.api.service.dataformats.log.LogParserService;

import java.io.IOException;
import java.util.List;

public class LogParserProcessor extends SingleLaneRecordProcessor {

  private final String fieldPathToParse;
  private final String parsedFieldPath;

  public LogParserProcessor(String fieldPathToParse, String parsedFieldPath) {
    this.fieldPathToParse = fieldPathToParse;
    this.parsedFieldPath = parsedFieldPath;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    return issues;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Field field = record.get(fieldPathToParse);
    if (field == null) {
      throw new OnRecordErrorException(Errors.LOGP_00, fieldPathToParse, record.getHeader().getSourceId());
    } else {
      String value = field.getValueAsString();
      if (value == null) {
        throw new OnRecordErrorException(Errors.LOGP_01, fieldPathToParse, record.getHeader().getSourceId());
      }
      try (DataParser parser = getContext().getService(LogParserService.class).getLogParser(
          record.getHeader()
                .getSourceId(),
          value
      )) {
        Record r = parser.parse();
        if(r != null) {
          Field parsed = r.get();
          record.set(parsedFieldPath, parsed);
        }
      } catch (IOException | DataParserException ex) {
        throw new OnRecordErrorException(
            Errors.LOGP_03,
            fieldPathToParse,
            record.getHeader().getSourceId(),
            ex.toString(),
            ex
        );
      }
      if (!record.has(parsedFieldPath)) {
        throw new OnRecordErrorException(Errors.LOGP_02, parsedFieldPath, record.getHeader().getSourceId());
      }
      batchMaker.addRecord(record);
    }
  }

}
