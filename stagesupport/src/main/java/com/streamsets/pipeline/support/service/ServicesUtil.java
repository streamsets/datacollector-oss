/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.support.service;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ToErrorContext;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;
import com.streamsets.pipeline.api.service.dataformats.DataParser;
import com.streamsets.pipeline.api.service.dataformats.DataParserException;
import com.streamsets.pipeline.api.service.dataformats.RecoverableDataParserException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Few common operations that stage developers might need to do with our default services
 */
public final class ServicesUtil {

  /**
   * Convenience method to parse all available records in given payload. This method assumes that the stage already
   * depends on DataFormatParserService service and loads the service instance from Stage.Context.
   *
   * @param stageContext Stage's context
   * @param toErrorContext Error context for any parser errors
   * @param produceSingleRecordPerMessage If set, then output will be list of exactly one record
   * @param messageId Message id base to be used with generated records
   * @param payload Binary payload to parse
   * @return
   * @throws StageException
   */
  public static List<Record> parseAll(
    Stage.Context stageContext,
    ToErrorContext toErrorContext,
    boolean produceSingleRecordPerMessage,
    String messageId,
    byte[] payload
  ) throws StageException {
    List<Record> records = new ArrayList<>();
    try (DataParser parser = stageContext.getService(DataFormatParserService.class).getParser(messageId, payload)) {
      Record record = null;
      boolean recoverableExceptionHit;
      do {
        try {
          recoverableExceptionHit = false;
          record = parser.parse();
        } catch (RecoverableDataParserException e) {
          handleException(stageContext, toErrorContext, messageId, e, e.getUnparsedRecord());
          recoverableExceptionHit = true;
          //Go to next record
          continue;
        }
        if (record != null) {
          records.add(record);
        }
      } while (record != null || recoverableExceptionHit);
    } catch (IOException |DataParserException ex) {
      Record record = stageContext.createRecord(messageId);
      record.set(Field.create(payload));
      handleException(stageContext, toErrorContext, messageId, ex, record);
      return records;
    }
    if (produceSingleRecordPerMessage) {
      List<Field> list = new ArrayList<>();
      for (Record record : records) {
        list.add(record.get());
      }
      Record record = records.get(0);
      record.set(Field.create(list));
      records.clear();
      records.add(record);
    }
    return records;
  }

  private static void handleException(
    Stage.Context stageContext,
    ToErrorContext errorContext,
    String messageId,
    Exception ex,
    Record record
  ) throws StageException {
    switch (stageContext.getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        errorContext.toError(record, ServiceErrors.SERVICE_ERROR_001, messageId, ex.toString(), ex);
        break;
      case STOP_PIPELINE:
        if (ex instanceof StageException) {
          throw (StageException) ex;
        } else {
          throw new StageException(ServiceErrors.SERVICE_ERROR_001, messageId, ex.toString(), ex);
        }
      default:
        throw new IllegalStateException(Utils.format("Unknown on error value '{}'", stageContext, ex));
    }
  }
}
