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

package com.streamsets.pipeline.stage.processor.jsongenerator;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.lib.generator.json.JsonCharDataGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;

public class JsonGeneratorProcessor extends SingleLaneRecordProcessor {
  private static final List<Field.Type> supportedFieldTypes = ImmutableList.of(
      Field.Type.MAP,
      Field.Type.LIST_MAP,
      Field.Type.LIST
  );

  private final String fieldPathToSerialize;
  private final String outputFieldPath;

  public JsonGeneratorProcessor(String fieldPathToSerialize, String outputFieldPath) {
    this.fieldPathToSerialize = fieldPathToSerialize;
    this.outputFieldPath = outputFieldPath;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    if (!record.has(fieldPathToSerialize)) {
      throw new OnRecordErrorException(Errors.JSON_00, fieldPathToSerialize, record.getHeader().getSourceId());
    }

    Field field = record.get(fieldPathToSerialize);
    if (field.getValue() == null) {
      throw new OnRecordErrorException(Errors.JSON_01, fieldPathToSerialize);
    }
    if (!supportedFieldTypes.contains(field.getType())) {
      throw new OnRecordErrorException(Errors.JSON_02, fieldPathToSerialize, field.getType());
    }

    Record tempRecord = getContext().createRecord(record.getHeader().getSourceId());
    tempRecord.set(field);
    Writer writer = new StringWriter();
    try (JsonCharDataGenerator generator = new JsonCharDataGenerator(getContext(), writer, Mode.MULTIPLE_OBJECTS)) {
      generator.write(tempRecord);
    } catch (IOException e) {
      throw new OnRecordErrorException(Errors.JSON_03, e.toString(), e);
    }

    record.set(outputFieldPath, Field.create(writer.toString().trim()));
    batchMaker.addRecord(record);
  }

}
