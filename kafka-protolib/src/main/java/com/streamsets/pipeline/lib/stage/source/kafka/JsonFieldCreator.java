/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.io.CountingReader;
import com.streamsets.pipeline.lib.json.OverrunStreamingJsonParser;
import com.streamsets.pipeline.lib.json.StreamingJsonParser;
import com.streamsets.pipeline.lib.stage.source.util.JsonUtil;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;


public class JsonFieldCreator implements FieldCreator {

  private final StreamingJsonParser.Mode jsonContent;
  private final int maxJsonObjectLen;

  public JsonFieldCreator(StreamingJsonParser.Mode jsonContent, int maxJsonObjectLen) {
    this.jsonContent = jsonContent;
    this.maxJsonObjectLen = maxJsonObjectLen;
  }

  @Override
  public Field createField(byte[] bytes) throws StageException {
    try (CountingReader reader =
           new CountingReader(new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bytes))))) {
      OverrunStreamingJsonParser parser = new OverrunStreamingJsonParser(reader, jsonContent, maxJsonObjectLen);
      return JsonUtil.jsonToField(parser.read());
    } catch (Exception e) {
      throw new StageException(null, e.getMessage(), e);
    }
  }
}
