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
package com.streamsets.datacollector.el;

import com.streamsets.datacollector.definition.ConcreteELDefinitionExtractor;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

public class TestTimeNewEL {

  ELEvaluator eval = new ELEvaluator("testTimeNowELFunctions", ConcreteELDefinitionExtractor.get(), TimeNowEL.class, RecordEL.class);
  ELVariables variables = new ELVariables();

  Date date;

  @Before
  public void setUp() {
    date = new Date();
    eval = new ELEvaluator("test", ConcreteELDefinitionExtractor.get(), TimeNowEL.class, RecordEL.class);
    variables = new ELVariables();
    variables.addContextVariable(TimeNowEL.TIME_NOW_CONTEXT_VAR, date);
    TimeNowEL.setTimeNowInContext(variables, date);
  }

  @Test
  public void testNow() throws Exception {
    Assert.assertEquals(date, eval.eval(variables, "${time:now()}", Date.class));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testTrimDate() throws Exception {
    Date output = eval.eval(variables, "${time:trimDate(time:now())}", Date.class);
    Assert.assertEquals(70, output.getYear());
    Assert.assertEquals(0, output.getMonth());
    Assert.assertEquals(1, output.getDate());

    Assert.assertNull(eval.eval(variables, "${time:trimDate(null)}", Date.class));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testTrimTime() throws Exception {
    Date output = eval.eval(variables, "${time:trimTime(time:now())}", Date.class);
    Assert.assertEquals(0, output.getHours());
    Assert.assertEquals(0, output.getMinutes());
    Assert.assertEquals(0, output.getSeconds());

    Assert.assertNull(eval.eval(variables, "${time:trimTime(null)}", Date.class));
  }

  @Test
  public void testMillisecondsToDateTime() throws Exception {

    Record.Header header = Mockito.mock(Record.Header.class);
    Record record = Mockito.mock(Record.class);
    Mockito.when(record.getHeader()).thenReturn(header);
    long val = System.currentTimeMillis();
    Mockito.when(record.get(Mockito.eq("/epochMS"))).thenReturn(Field.create(val));

    RecordEL.setRecordInContext(variables, record);

    Assert.assertEquals(new Date(val), eval.eval(variables, "${time:millisecondsToDateTime(record:value('/epochMS'))}", Date.class));
  }

  @Test
  public void testDateTimeToMilliSeconds() throws Exception {
    Record.Header header = Mockito.mock(Record.Header.class);
    Record record = Mockito.mock(Record.class);
    Mockito.when(record.getHeader()).thenReturn(header);
    Long val = System.currentTimeMillis();
    Mockito.when(record.get(Mockito.eq("/date"))).thenReturn(Field.create(Field.Type.DATETIME, new Date(val)));
    RecordEL.setRecordInContext(variables, record);
    Assert.assertEquals(val, eval.eval(variables, "${time:dateTimeToMilliseconds(record:value('/date'))}", Long.class));
  }

  @Test
  public void testExtractStringFromDate() throws Exception {

    Record.Header header = Mockito.mock(Record.Header.class);
    Record record = Mockito.mock(Record.class);
    Mockito.when(record.getHeader()).thenReturn(header);
    long val = System.currentTimeMillis();
    Date d = new Date(val);
    Mockito.when(record.get(Mockito.eq("/theDate"))).thenReturn(Field.createDate(d));
    Mockito.when(record.get(Mockito.eq("/epochMS"))).thenReturn(Field.create(val));

    String format  = "G yyyy-MM-dd HH:mm:ss.SSS a zzzzzzz";
    Mockito.when(record.get(Mockito.eq("/format"))).thenReturn(Field.create(format));

    RecordEL.setRecordInContext(variables, record);

    SimpleDateFormat sdf = new SimpleDateFormat(format);

    String ans = sdf.format(d);

    Assert.assertEquals(ans, eval.eval(variables,
        "${time:extractStringFromDate(time:millisecondsToDateTime(record:value('/epochMS')), record:value('/format'))}", String.class));

    Assert.assertEquals(ans, eval.eval(variables,
        "${time:extractStringFromDate(record:value('/theDate'), record:value('/format'))}", String.class));
  }

  private void checkExtractDateFromString(Date date, String dateString, String format) throws Exception {
    Assert.assertEquals(date, TimeNowEL.extractDateFromString(dateString, format));
  }

  @Test
  public void testExtractDateFromString() throws Exception {
    Calendar calendar = Calendar.getInstance();
    calendar.set(Calendar.MILLISECOND, 915);
    calendar.set(Calendar.SECOND, 30);
    calendar.set(Calendar.MINUTE, 15);
    calendar.set(Calendar.HOUR_OF_DAY, 20);
    calendar.set(Calendar.DAY_OF_MONTH, 1);
    calendar.set(Calendar.MONTH, 4);
    calendar.set(Calendar.YEAR, 2017);

    date = calendar.getTime();
    TimeNowEL.setTimeNowInContext(variables, date);

    checkExtractDateFromString(date,"2017/05/01 20:15:30.915", "yyyy/MM/dd HH:mm:ss.SSS");
    checkExtractDateFromString(new Date((date.getTime()/1000) * 1000),"2017/05/01 20:15:30", "yy/MM/dd HH:mm:ss");
    checkExtractDateFromString(
        new SimpleDateFormat("HH:mm:ss.SSS").parse("20:15:30.915"),
        "20:15:30.915",
        "HH:mm:ss.SSS"
    );

    checkExtractDateFromString(
        new SimpleDateFormat("yyyy-MM-dd").parse("2017-05-01"),
        "01-May-2017",
        "dd-MMM-yyyy"
    );

    checkExtractDateFromString(
        new SimpleDateFormat("yyyy/MMM/dd HH:mm:ss.SSS Z", Locale.US).parse("2017/May/01 20:15:30.915 PDT"),
        "2017-05-01 20:15:30.915 PDT",
        "yyyy-MM-dd HH:mm:ss.SSS Z"
    );
  }


  @Test
  public void testExtractLongFromDate() throws Exception {

    Record.Header header = Mockito.mock(Record.Header.class);
    Record record = Mockito.mock(Record.class);
    Mockito.when(record.getHeader()).thenReturn(header);

    long val = System.currentTimeMillis();
    Mockito.when(record.get(Mockito.eq("/epochMS"))).thenReturn(Field.create(val));

    String format = "yyyy/MM/dd HH:mm:ss.SSS";
    Mockito.when(record.get(Mockito.eq("/format"))).thenReturn(Field.create(format));

    RecordEL.setRecordInContext(variables, record);

    SimpleDateFormat sdf = new SimpleDateFormat(format);

    Date d = new Date(val);
    String digits = sdf.format(d).replaceAll("[^0-9]","");
    Long ans = Long.parseLong(digits);

    Assert.assertEquals(ans, eval.eval(variables,
        "${time:extractLongFromDate(time:millisecondsToDateTime(record:value('/epochMS')), record:value('/format'))}", Long.class));
  }

}
