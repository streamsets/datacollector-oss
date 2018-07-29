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
package com.streamsets.pipeline.stage.processor.aggregation;

import org.junit.Assert;
import org.junit.Test;

import java.util.Calendar;
import java.util.TimeZone;

public class TestTimeWindow {

  @Test
  public void testGetNextWindowCloseTimeMillis() {

    Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    long windowStart = calendar.getTimeInMillis();

    // current time is at beginning of time window
    long currentTime = windowStart;
    for (TimeWindow timeWindow : TimeWindow.values()) {
      long expected = windowStart + timeWindow.getUnit().toMillis(timeWindow.getInterval());
      Assert.assertEquals(expected, timeWindow.getCurrentWindowCloseTimeMillis(TimeZone.getDefault(), currentTime));
    }

    // current time is within the time window
    calendar.set(Calendar.SECOND, 3);
    currentTime = calendar.getTimeInMillis();
    for (TimeWindow timeWindow : TimeWindow.values()) {
      long expected = windowStart + timeWindow.getUnit().toMillis(timeWindow.getInterval());
      Assert.assertEquals(expected, timeWindow.getCurrentWindowCloseTimeMillis(TimeZone.getDefault(), currentTime));
    }

    calendar.set(Calendar.SECOND, 16);
    currentTime = calendar.getTimeInMillis();
    long expected = windowStart + TimeWindow.TW_5S.getUnit().toMillis(20);
    Assert.assertEquals(expected, TimeWindow.TW_5S.getCurrentWindowCloseTimeMillis(TimeZone.getDefault(), currentTime));

    calendar.set(Calendar.MINUTE, 7);
    currentTime = calendar.getTimeInMillis();
    expected = windowStart + TimeWindow.TW_5M.getUnit().toMillis(10);
    Assert.assertEquals(expected, TimeWindow.TW_5M.getCurrentWindowCloseTimeMillis(TimeZone.getDefault(), currentTime));

    calendar.set(Calendar.HOUR_OF_DAY, 9);
    currentTime = calendar.getTimeInMillis();
    expected = windowStart + TimeWindow.TW_6H.getUnit().toMillis(12);
    Assert.assertEquals(expected, TimeWindow.TW_6H.getCurrentWindowCloseTimeMillis(TimeZone.getDefault(), currentTime));

  }

  @Test
  public void testMicroWindows() {
    Assert.assertNull(TimeWindow.TW_5S.getMicroTimeWindow());
    Assert.assertNull(TimeWindow.TW_10S.getMicroTimeWindow());
    Assert.assertNull(TimeWindow.TW_30S.getMicroTimeWindow());
    Assert.assertNotNull(TimeWindow.TW_1M.getMicroTimeWindow());
    Assert.assertNotNull(TimeWindow.TW_5M.getMicroTimeWindow());
    Assert.assertNotNull(TimeWindow.TW_10M.getMicroTimeWindow());
    Assert.assertNull(TimeWindow.TW_15M.getMicroTimeWindow());
    Assert.assertNotNull(TimeWindow.TW_30M.getMicroTimeWindow());
    Assert.assertNotNull(TimeWindow.TW_1H.getMicroTimeWindow());
    Assert.assertNotNull(TimeWindow.TW_6H.getMicroTimeWindow());
    Assert.assertNull(TimeWindow.TW_8H.getMicroTimeWindow());
    Assert.assertNotNull(TimeWindow.TW_12H.getMicroTimeWindow());
    Assert.assertNotNull(TimeWindow.TW_1D.getMicroTimeWindow());

    Assert.assertEquals(60 / 5, TimeWindow.TW_1M.getNumberOfMicroTimeWindows());
    Assert.assertEquals(60 * 5 / 5, TimeWindow.TW_5M.getNumberOfMicroTimeWindows());
    Assert.assertEquals(60 * 10 / 10, TimeWindow.TW_10M.getNumberOfMicroTimeWindows());
    Assert.assertEquals(60 * 30 / 30, TimeWindow.TW_30M.getNumberOfMicroTimeWindows());
    Assert.assertEquals(60, TimeWindow.TW_1H.getNumberOfMicroTimeWindows());
    Assert.assertEquals(60 * 6 / 5, TimeWindow.TW_6H.getNumberOfMicroTimeWindows());
    Assert.assertEquals(60 * 12 / 10, TimeWindow.TW_12H.getNumberOfMicroTimeWindows());
    Assert.assertEquals(60 * 24 / 20, TimeWindow.TW_1D.getNumberOfMicroTimeWindows());
  }
}
