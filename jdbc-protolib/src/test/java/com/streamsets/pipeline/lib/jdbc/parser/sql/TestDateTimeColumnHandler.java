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

import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Date;


public class TestDateTimeColumnHandler {
  private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  @Test
  public void testOracleDate() throws StageException, ParseException {
    String testDateStringBase    = "10-04-2018 02:15:10";
    String testDateStringTimeZone = "GMT";
    String testDateString = testDateStringBase + " " + testDateStringTimeZone;
    String testDateStringPattern = "dd-MM-yyyy HH:mm:ss z"; // z is for timezone

    String testColumnValue = "TO_DATE('" + testDateStringBase + "', 'DD-MM-YYYY HH24:MI:SS')";
    String expectedOutputPattern = "EEE LLL dd HH:mm:ss zzz yyyy";

    DateTimeColumnHandler handler = new DateTimeColumnHandler(ZoneId.of(testDateStringTimeZone), false);

    // A TIME data type in Oracle is indeed a timestamp. The difference with respect to Oracle TIMESTAMP is that
    // TIME does not store fractional seconds.
    Field field = handler.getDateTimeStampField(
        "dt",
        testColumnValue,
        Types.TIMESTAMP
    );
    Assert.assertEquals(Field.Type.DATETIME, field.getType());
    Date date = new SimpleDateFormat(testDateStringPattern).parse(testDateString);
    String dateStr = new SimpleDateFormat(expectedOutputPattern).format(date);
    Assert.assertEquals(dateStr, field.getValueAsDatetime().toString());
  }

  @Test
  public void testTimestampWithFieldAttribute() throws StageException {
    DateTimeColumnHandler handler = new DateTimeColumnHandler(ZoneId.of("GMT"), false);
    Field field = handler.getDateTimeStampField(
        "dt",
        "TO_TIMESTAMP('2018-04-10 02:15:10.654321')",
        Types.TIMESTAMP
    );
    Assert.assertEquals(Field.Type.DATETIME, field.getType());
    Assert.assertEquals("2018-04-10 02:15:10", dateFormat.format(field.getValueAsDatetime()));
    Assert.assertEquals("321000", field.getAttribute("nanoSeconds"));
  }

  @Test
  public void testTimestampWithFieldAsString() throws StageException {
    DateTimeColumnHandler handler = new DateTimeColumnHandler(ZoneId.of("GMT"), true);

    Field field = handler.getDateTimeStampField(
        "dt",
        "TO_TIMESTAMP('2018-04-10 02:15:10.654321')",
        Types.TIMESTAMP
    );
    Assert.assertEquals(Field.Type.STRING, field.getType());
    Assert.assertEquals("2018-04-10 02:15:10.654321", field.getValueAsString());
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
