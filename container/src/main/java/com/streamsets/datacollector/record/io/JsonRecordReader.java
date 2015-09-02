/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.RecordJson;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.ext.RecordReader;
import com.streamsets.pipeline.lib.io.CountingReader;
import com.streamsets.pipeline.lib.json.OverrunStreamingJsonParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class JsonRecordReader extends OverrunStreamingJsonParser implements RecordReader {

  public JsonRecordReader(InputStream inputStream, long initialPosition, int maxObjectLen) throws IOException {
    super(new CountingReader(new InputStreamReader(inputStream, "UTF-8")), initialPosition,
          Mode.MULTIPLE_OBJECTS, maxObjectLen);
  }

  @Override
  public String getEncoding() {
    return RecordEncoding.JSON1.name();
  }

  @Override
  protected ObjectMapper getObjectMapper() {
    return ObjectMapperFactory.get();
  }

  @Override
  protected Class<?> getExpectedClass() {
    return RecordJson.class;
  }

  @Override
  public long getPosition() {
    return getReaderPosition();
  }

  @Override
  public Record readRecord() throws IOException {
    RecordJson recordJson = (RecordJson) read();
    return BeanHelper.unwrapRecord(recordJson);
  }
}
