/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
