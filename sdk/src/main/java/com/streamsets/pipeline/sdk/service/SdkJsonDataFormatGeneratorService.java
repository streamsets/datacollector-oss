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
package com.streamsets.pipeline.sdk.service;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.BaseService;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.JsonRecordWriter;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.api.service.ServiceDef;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;
import com.streamsets.pipeline.api.service.dataformats.DataGenerator;
import com.streamsets.pipeline.api.service.dataformats.DataGeneratorException;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;


@ServiceDef(
  provides = DataFormatGeneratorService.class,
  version = 1,
  label = "(Test) Runner implementation of very simple DataFormatGeneratorService that will always output JSON."
)
public class SdkJsonDataFormatGeneratorService extends BaseService implements DataFormatGeneratorService {

  @Override
  public DataGenerator getGenerator(OutputStream os) throws IOException {
    ContextExtensions ext = ((ContextExtensions) getContext());
    JsonRecordWriter recordWriter = ext.createJsonRecordWriter(new OutputStreamWriter(os, Charset.defaultCharset()), Mode.MULTIPLE_OBJECTS);

    return new DataGeneratorImpl(recordWriter);
  }

  private static class DataGeneratorImpl implements DataGenerator {

    private final JsonRecordWriter recordWriter;
    DataGeneratorImpl(JsonRecordWriter recordWriter) {
      this.recordWriter = recordWriter;
    }

    @Override
    public void write(Record record) throws IOException, DataGeneratorException {
      recordWriter.write(record);
    }

    @Override
    public void flush() throws IOException {
      recordWriter.flush();
    }

    @Override
    public void close() throws IOException {
      recordWriter.close();
    }
  }
}
