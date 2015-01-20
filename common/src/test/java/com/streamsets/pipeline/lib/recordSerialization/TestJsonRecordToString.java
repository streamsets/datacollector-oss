/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.recordSerialization;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.recordserialization.JsonRecordToString;
import com.streamsets.pipeline.lib.recordserialization.RecordToString;
import com.streamsets.pipeline.lib.util.JsonUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestJsonRecordToString {

  private static final String expected = "{\"phone\":[\"1234567890\",\"0987654321\"]," +
    "\"address\":[{\"street\":\"180 Sansome\",\"city\":\"San Francisco\"},{\"street\":\"200 Sansome\"," +
    "\"city\":\"San Francisco\"}],\"age\":21,\"name\":\"xyz\"}";

  @Test(expected = IllegalArgumentException.class)
  public void testRecordToStringWithMapping() throws IOException, StageException {
    RecordToString recordToString = new JsonRecordToString();
    Map<String, String> fieldPathToName = new LinkedHashMap<>();
    fieldPathToName.put("/age", "years");
    recordToString.setFieldPathToNameMapping(fieldPathToName);
  }

  @Test
  public void testRecordToString() throws IOException, StageException {
    RecordToString recordToString = new JsonRecordToString();
    String actual = recordToString.toString(createJsonRecordWithMap());
    Assert.assertEquals(expected, actual);
  }

  private static Record createJsonRecordWithMap() throws IOException {
    TypeReference<List<HashMap<String,Object>>> typeRef
      = new TypeReference<List<HashMap<String,Object>>>() {};
    List<Map<String, String>> o = new ObjectMapper().readValue(TestJsonRecordToString.class.getClassLoader()
      .getResourceAsStream("jsonData.json"), typeRef);
    Record record = Mockito.mock(Record.class);
    Field f = JsonUtil.jsonToField(o.get(0));
    Mockito.when(record.get()).thenReturn(f);
    return record;
  }

}
