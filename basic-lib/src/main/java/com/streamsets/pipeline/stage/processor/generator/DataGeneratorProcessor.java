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
package com.streamsets.pipeline.stage.processor.generator;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;
import com.streamsets.pipeline.api.service.dataformats.DataGenerator;
import com.streamsets.pipeline.api.service.dataformats.DataGeneratorException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class DataGeneratorProcessor extends SingleLaneRecordProcessor {

  private final DataGeneratorConfig config;

  public DataGeneratorProcessor(DataGeneratorConfig config) {
    this.config = config;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    try {
      // Serialize record to byte array
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      DataGenerator generator = getContext().getService(DataFormatGeneratorService.class).getGenerator(outputStream);
      generator.write(record);
      generator.close();
      byte[] output = outputStream.toByteArray();

      // And update the resulting record
      switch (config.outputType) {
        case STRING:
          record.set(config.targetField, Field.create(Field.Type.STRING, new String(output)));
          break;
        case BYTE_ARRAY:
          record.set(config.targetField, Field.create(Field.Type.BYTE_ARRAY, output));
          break;
        default:
          throw new IllegalArgumentException("Unknown OutputType: " + config.outputType);
      }
      batchMaker.addRecord(record);
    } catch (IOException|DataGeneratorException e) {
      throw new OnRecordErrorException(record, Errors.GENERATOR_001, e.toString(), e);
    }
  }
}
