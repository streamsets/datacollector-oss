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
package com.streamsets.pipeline.lib.generator.sdcrecord;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordWriter;
import com.streamsets.pipeline.lib.generator.DataGenerator;

import java.io.IOException;

public class SdcRecordDataGenerator implements DataGenerator {

  private final RecordWriter writer;
  private final ContextExtensions context;

  public SdcRecordDataGenerator(RecordWriter writer, ContextExtensions context)
      throws IOException {
    this.writer = writer;
    this.context = context;
  }

  @Override
  public void write(Record record) throws IOException {
    if (null != context.getSampler()) {
      context.getSampler().sample(record);
    }
    writer.write(record);
  }

  @Override
  public void flush() throws IOException {
    writer.flush();
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
