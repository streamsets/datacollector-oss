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
package com.streamsets.pipeline.lib.el;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class TestTimeNowEL {

  @Test
  public void testTimeToAmerica_New_York() {

    // 1484320146987   GMT: Fri, 13 Jan 2017 15:09:06.987 GMT
    Assert.assertEquals("2017-01-13 10:09:06.987",
        TimeNowEL.extractStringFromDateTZ(new Date(1484320146987L), "America/New_York", "yyyy-MM-dd HH:mm:ss.SSS")
    );
  }

  @Test
  public void testTimeToGMT() {

    // 1484320146987   GMT: Fri, 13 Jan 2017 15:09:06.987 GMT
    Assert.assertEquals("2017-01-13 15:09:06",
        TimeNowEL.extractStringFromDateTZ(new Date(1484320146987L), "GMT", "yyyy-MM-dd HH:mm:ss")
    );
  }

  @Test
  public void testTimeToGMT_0800() {

    // 1484320146987   GMT: Fri, 13 Jan 2017 15:09:06.987 GMT
    Assert.assertEquals("2017-01-13 07:09:06 GMT-08:00",
        TimeNowEL.extractStringFromDateTZ(new Date(1484320146987L), "GMT-0800", "yyyy-MM-dd HH:mm:ss zzz")
    );
    Assert.assertEquals("2017-01-13 07:09:06 GMT-08:00",
        TimeNowEL.extractStringFromDateTZ(new Date(1484320146987L), "GMT-08:00", "yyyy-MM-dd HH:mm:ss zzz")
    );
  }

  @Test
  public void testTimeTo_0500() {

    // 1484320146987   GMT: Fri, 13 Jan 2017 15:09:06.987 GMT
    // must be GMT-05:00 not -05:00
    Assert.assertEquals("",
        TimeNowEL.extractStringFromDateTZ(new Date(1484320146987L), "-05:00", "yyyy-MM-dd HH:mm:ss ZZZ")
    );
  }

  @Test
  public void testTimeTo_EDT() {

    // 1484320146987   GMT: Fri, 13 Jan 2017 15:09:06.987 GMT
    // EDT is not valid.
    Assert.assertEquals("",
        TimeNowEL.extractStringFromDateTZ(new Date(1484320146987L), "EDT", "yyyy-MM-dd HH:mm:ss ZZZ")
    );
  }

  @Test
  public void testTimeTo_EST() {

    // 1484320146987   GMT: Fri, 13 Jan 2017 15:09:06.987 GMT
    //  EST is valid 3-letter short timezone but gives the wrong result for DST.  This non-DST date works...
    Assert.assertEquals("2017-01-13 10:09:06 -0500",
        TimeNowEL.extractStringFromDateTZ(new Date(1484320146987L), "EST", "yyyy-MM-dd HH:mm:ss ZZZ")
    );

    // This date in US daylight savings time does not work.
    // 1472393106987 Sun, 28 Aug 2016 14:05:06 GMT
    // should by 10:05:06, but comes out as 09:05:06.  and -0500 is misleading. should be -0400
    Assert.assertEquals("2016-08-28 09:05:06 -0500",
        TimeNowEL.extractStringFromDateTZ(new Date(1472393106987L), "EST", "yyyy-MM-dd HH:mm:ss ZZZZZ")
    );
  }

  @Test
  public void testTimeTo_EST5EDT() {

    // 1484320146987   GMT: Fri, 13 Jan 2017 15:09:06.987 GMT
    //  EST5EDT works for both daylight savings and standard time.
    Assert.assertEquals("2017-01-13 10:09:06 Eastern Standard Time",
        TimeNowEL.extractStringFromDateTZ(new Date(1484320146987L), "EST5EDT", "yyyy-MM-dd HH:mm:ss zzzzz")
    );

    // This date in US daylight savings time does not work.
    // 1472393106987 Sun, 28 Aug 2016 14:05:06 GMT
    Assert.assertEquals("2016-08-28 10:05:06 Eastern Daylight Time",
        TimeNowEL.extractStringFromDateTZ(new Date(1472393106987L), "EST5EDT", "yyyy-MM-dd HH:mm:ss zzzzz")
    );

  }

  @Test
  public void testTimeTo_America_Los_Angeles() {

    // 1484320146987   GMT: Fri, 13 Jan 2017 15:09:06.987 GMT
    // America/Los_Angeles works for both daylight savings and standard time.
    Assert.assertEquals("2017-01-13 07:09:06 Pacific Standard Time",
        TimeNowEL.extractStringFromDateTZ(new Date(1484320146987L), "America/Los_Angeles", "yyyy-MM-dd HH:mm:ss zzzzz")
    );

    // This date is in US daylight savings time.
    // 1472393106987 Sun, 28 Aug 2016 14:05:06 GMT
    Assert.assertEquals("2016-08-28 07:05:06 Pacific Daylight Time",
        TimeNowEL.extractStringFromDateTZ(new Date(1472393106987L), "America/Los_Angeles", "yyyy-MM-dd HH:mm:ss zzzzz")
    );

  }

  @Test
  public void testTimeToAsiaRiyadh() {

    // 1484320146987 GMT: Fri, 13 Jan 2017 15:09:06.987 GMT
    // AST: 1/13/2017, 18:09:06 AM GMT-5:00

    Assert.assertEquals("2017-01-13 18:09:06 +0300",
        TimeNowEL.extractStringFromDateTZ(new Date(1484320146987L), "Asia/Riyadh", "yyyy-MM-dd HH:mm:ss ZZZZZ")
    );
  }

  @Test
  public void testCreateDatefromStringTZ() {

    String in = "2016-07-14 11:23:45";
    String tz = "Pacific/Honolulu";
    String fmt = "yyyy-MM-dd HH:mm:ss";
    String fmt2 = "yyyy-MM-dd HH:mm:ss zzzz";

    Date dt = TimeNowEL.createDateFromStringTZ(in, tz, fmt);
    Assert.assertNotNull(dt);


    // prevents conversion to local time.
    SimpleDateFormat formatter = new SimpleDateFormat(fmt2);
    TimeZone theZone = TimeZone.getTimeZone(tz);
    formatter.setTimeZone(theZone);

    Assert.assertEquals("2016-07-14 11:23:45 Hawaii Standard Time", formatter.format(dt));
  }

  @Test
  public void testTimeZoneOffset() {
    long HOUR = 60*60*1000;
    long HALF = 30*60*1000;

    // Pure timezone
    Assert.assertEquals(0, TimeNowEL.timeZoneOffset("GMT"));
    Assert.assertEquals(-5*HOUR, TimeNowEL.timeZoneOffset("GMT-05:00"));
    Assert.assertEquals(1*HOUR, TimeNowEL.timeZoneOffset("GMT+01:00"));
    Assert.assertEquals(5*HOUR+HALF, TimeNowEL.timeZoneOffset("IST"));

    // And offset for a given date
    Date july = new Date(2018 - 1900, 8, 1);
    Date february = new Date(2018 - 1900, 1, 1);

    Assert.assertEquals(-7*HOUR, TimeNowEL.timeZoneOffset(july,"PST")); // Daylight saving time
    Assert.assertEquals(-8*HOUR, TimeNowEL.timeZoneOffset(february,"PST"));
  }

  @Test
  public void testExtractNanosecondsFromString() {
    String timestampNanosWithoutMillis = "105000";
    String timestampWithoutNanosStr = "2005-02-08 14:00:00.100";
    String timestamp = "2005-02-08 14:00:00.100105000";

    Timestamp timestampWithoutNanos = Timestamp.valueOf(timestampWithoutNanosStr);

    String parsedTimestamp = TimeNowEL.extractNanosecondsFromString(timestamp);
    String[] timestampParts = parsedTimestamp.split(TimeNowEL.OFFSET_VALUE_NANO_SEPARATOR);

    Assert.assertEquals(2, timestampParts.length);
    Assert.assertEquals(timestampNanosWithoutMillis, timestampParts[1]);
    Assert.assertEquals(timestampWithoutNanos.getTime(), Long.valueOf(timestampParts[0]).longValue());

    timestamp = "2005-02-08 14:00:00.100";

    parsedTimestamp = TimeNowEL.extractNanosecondsFromString(timestamp);
    timestampParts = parsedTimestamp.split(TimeNowEL.OFFSET_VALUE_NANO_SEPARATOR);

    Assert.assertEquals(1, timestampParts.length);
    Assert.assertEquals(timestampWithoutNanos.getTime(), Long.valueOf(timestampParts[0]).longValue());
  }
}
