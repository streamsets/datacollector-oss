/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.lib.jdbc.parser.sql;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import org.junit.Assert;
import org.junit.Test;
import java.time.ZoneId;

public class TestDateTimeColumnHandler {

  @Test
  public void testTimestampWithFieldAttribute() {
    DateTimeColumnHandler handler = new DateTimeColumnHandler(ZoneId.of("GMT"), false);
    try {
      Field field = handler.getDateTimeStampField(
          "dt",
          "TO_TIMESTAMP('2018-04-10 02:15:10.654321')",
          0 ,
          "TIMESTAMP"
      );
      Assert.assertEquals(Field.Type.DATETIME, field.getType());
      Assert.assertEquals("Tue Apr 10 02:15:10 UTC 2018", field.getValueAsDatetime().toString());
      Assert.assertEquals("321000", field.getAttribute("nanoSeconds"));
    } catch (StageException ex) {
      Assert.fail("StageException should not be thrown");
    }
  }

  @Test
  public void testTimestampWithFieldAsString() {
    DateTimeColumnHandler handler = new DateTimeColumnHandler(ZoneId.of("GMT"), true);
    try {
      Field field = handler.getDateTimeStampField(
          "dt",
          "TO_TIMESTAMP('2018-04-10 02:15:10.654321')",
          0 ,
          "TIMESTAMP"
      );
      Assert.assertEquals(Field.Type.STRING, field.getType());
      Assert.assertEquals("2018-04-10 02:15:10.654321", field.getValueAsString());
    } catch (StageException ex) {
      Assert.fail("StageException should not be thrown");
    }
  }

  @Test
  public void testZonedTimestampwithFieldAttribute() {
    DateTimeColumnHandler handler = new DateTimeColumnHandler(ZoneId.of("GMT"), false);
    Field field = handler.getTimestampWithTimezoneField("TO_TIMESTAMP_TZ('2000-10-28 23:24:54.123456 -04:00')");
    Assert.assertEquals(Field.Type.ZONED_DATETIME, field.getType());
    Assert.assertEquals("2000-10-28T23:24:54.123456-04:00", field.getValueAsZonedDateTime().toString());
  }

  @Test
  public void testZonedTimestampAsString() {
    DateTimeColumnHandler handler = new DateTimeColumnHandler(ZoneId.of("GMT"), true);

    Field field = handler.getTimestampWithTimezoneField( "TO_TIMESTAMP_TZ('2019-04-16 21:24:54.035821 +09:00')");
    Assert.assertEquals(Field.Type.STRING, field.getType());
    Assert.assertEquals("2019-04-16 21:24:54.035821 +09:00", field.getValueAsString());
  }

  @Test
  public void testLocalZonedTimestampwithFieldAttribute() {
    DateTimeColumnHandler handler = new DateTimeColumnHandler(ZoneId.of("Asia/Tokyo"), false);
    Field field = handler.getTimestampWithLocalTimezone("TO_TIMESTAMP_TZ('2019-10-28 23:24:54.123456')");
    Assert.assertEquals(Field.Type.ZONED_DATETIME, field.getType());
    Assert.assertEquals("2019-10-28T23:24:54.123456+09:00[Asia/Tokyo]", field.getValueAsZonedDateTime().toString());
  }

  @Test
  public void testLocalZonedTimestampAsString() {
    DateTimeColumnHandler handler = new DateTimeColumnHandler(ZoneId.of("Asia/Tokyo"), true);
    Field field = handler.getTimestampWithLocalTimezone("TO_TIMESTAMP_TZ('2019-10-28 23:24:54.123456')");
    Assert.assertEquals(Field.Type.STRING, field.getType());
    Assert.assertEquals("2019-10-28 23:24:54.123456", field.getValueAsString());
  }
}
