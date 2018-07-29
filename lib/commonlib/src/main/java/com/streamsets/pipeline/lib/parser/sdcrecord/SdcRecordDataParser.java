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
package com.streamsets.pipeline.lib.parser.sdcrecord;

import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordReader;
import com.streamsets.pipeline.api.ext.Sampler;
import com.streamsets.pipeline.lib.parser.AbstractDataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;

import java.io.IOException;
import java.io.InputStream;

public class SdcRecordDataParser extends AbstractDataParser {

  private final RecordReader recordReader;
  private boolean eof;
  private final ProtoConfigurableEntity.Context context;

  public SdcRecordDataParser(ProtoConfigurableEntity.Context context, InputStream inputStream, long readerOffset, int maxObjectLen)
      throws IOException {
    this.context = context;
    recordReader = ((ContextExtensions)context).createRecordReader(inputStream, readerOffset, maxObjectLen);
  }

  @Override
  public Record parse() throws IOException, DataParserException {
    Record record = recordReader.readRecord();
    eof = (record == null);
    if (null != record) {
      Sampler sampler = ((ContextExtensions) context).getSampler();
      if (null != sampler) {
        sampler.sample(record);
      }
    }
    return record;
  }

  @Override
  public String getOffset() {
    return (eof) ? String.valueOf(-1) : String.valueOf(recordReader.getPosition());
  }

  @Override
  public void close() throws IOException {
    recordReader.close();
  }
}
