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
package com.streamsets.datacollector.record.io;

import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.json.OverrunJsonObjectReaderImpl;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.RecordJson;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.ext.JsonObjectReader;
import com.streamsets.pipeline.api.ext.RecordReader;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.api.ext.io.CountingReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class SdcJsonRecordReader implements RecordReader {
  private final JsonObjectReader reader;

  public SdcJsonRecordReader(InputStream inputStream, long initialPosition, int maxObjectLen) throws IOException {
    reader = new OverrunJsonObjectReaderImpl(
        new OverrunReader(
            new CountingReader(new InputStreamReader(inputStream, "UTF-8")),
          OverrunReader.getDefaultReadLimit(),
          false,
          false
        ),
        initialPosition,
        maxObjectLen,
        Mode.MULTIPLE_OBJECTS,
        RecordJson.class,
        ObjectMapperFactory.get()
    );
  }

  @Override
  public String getEncoding() {
    return RecordEncoding.JSON1.name();
  }

  @Override
  public long getPosition() {
    return reader.getReaderPosition();
  }

  @Override
  public Record readRecord() throws IOException {
    Object record = reader.read();
    if(record == JsonObjectReader.EOF) {
      return null;
    }
    return BeanHelper.unwrapRecord((RecordJson)record);
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
