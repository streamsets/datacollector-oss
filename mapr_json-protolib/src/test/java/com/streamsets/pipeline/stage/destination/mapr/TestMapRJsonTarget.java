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
package com.streamsets.pipeline.stage.destination.mapr;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.testing.SingleForkNoReuseTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.streamsets.pipeline.stage.destination.mapr.MapRJsonTarget.convertToByteArray;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.fail;

@Category(SingleForkNoReuseTest.class)

public class TestMapRJsonTarget {

  @Test(timeout = 10000)
  public void testConvertToByteArray() throws Exception {

    Record rec = RecordCreator.create("s", "s:1");
    Map<String, Field> r = new LinkedHashMap<>();
    r.put("float", Field.create((float)3.14));
    r.put("double", Field.create((double)6.28));
    r.put("boolean", Field.create(true));
    r.put("char", Field.create('a'));
    r.put("byte", Field.create((byte) 0x41));
    r.put("int", Field.create(22));
    r.put("long", Field.create(27L));
    r.put("short", Field.create((short)32));
    r.put("string", Field.create("hello world"));

    Date date = new Date();
    r.put("date", Field.createDate(date));
    r.put("datetime", Field.createDatetime(date));

    rec.set(Field.create(r));
    Field f = null;

    byte [] ans = convertToByteArray(rec.get("/float"), rec);
    assertEquals("float length does not match", 4, ans.length);

    ans = convertToByteArray(rec.get("/double"), rec);
    assertEquals("double length does not match", 8, ans.length);

    // not supported; throws exception.
    try {
      ans = convertToByteArray(rec.get("/boolean"), rec);
      fail("boolean should have thrown an exception");
    } catch(OnRecordErrorException ex) {
      assertNotNull(ex.getMessage());
    }

    // not supported; throws exception.
    try {
      ans = convertToByteArray(rec.get("/char"), rec);
      fail("char should have thrown an exception");
    } catch(OnRecordErrorException ex) {
      assertNotNull(ex.getMessage());
    }

    // not supported; throws exception.
    try {
      ans = convertToByteArray(rec.get("/byte"), rec);
      fail("byte should have thrown an exception");
    } catch(OnRecordErrorException ex) {
      assertNotNull(ex.getMessage());
    }

    ans = convertToByteArray(rec.get("/int"), rec);
    assertEquals("integer length does not match", 4, ans.length);

    ans = convertToByteArray(rec.get("/long"), rec);
    assertEquals("long length does not match", 8, ans.length);

    ans = convertToByteArray(rec.get("/short"), rec);
    assertEquals("short length does not match", 2, ans.length);

    ans = convertToByteArray(rec.get("/string"), rec);
    assertEquals("String length does not match", 11, ans.length);

    ans = convertToByteArray(rec.get("/date"), rec);
    assertEquals("date length does not match", 8, ans.length);

    ans = convertToByteArray(rec.get("/datetime"), rec);
    assertEquals("datetime length does not match", 8, ans.length);

  }
}
