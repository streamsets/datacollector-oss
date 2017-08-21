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
package com.streamsets.pipeline.stage.destination.hdfs.writer;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsDTarget;
import org.junit.Assert;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class TestPathResolver {

  private Stage.Context getContext() {
    return ContextInfoCreator.createTargetContext(HdfsDTarget.class, "i", false, OnRecordError.DISCARD, "dir");
  }

  private PathResolver getPathTemplateEL(String pathTemplate) {
   PathResolver t = new PathResolver(getContext(), "dirPathTemplate", pathTemplate, TimeZone.getTimeZone("UTC"));
    Assert.assertTrue(t.validate("g", "c", "c", new ArrayList<Stage.ConfigIssue>()));
    return t;
  }

  private boolean validate(String pathTemplate) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    PathResolver t = new PathResolver(getContext(), "dirPathTemplate", pathTemplate, TimeZone.getTimeZone("UTC"));
    t.validate("g", "c", "c", issues);
    return issues.isEmpty();
  }

  @Test
  public void validateValid() {
    Assert.assertTrue(validate("/"));
    Assert.assertTrue(validate("/${YY()}"));
    Assert.assertTrue(validate("/${YYYY()}"));
    Assert.assertTrue(validate("/${YY()}${MM()}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${MM()}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${mm()}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${mm()}${ss()}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${mm()}${every(1, ss())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${mm()}${every(2, ss())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${mm()}${every(3, ss())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${mm()}${every(4, ss())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${mm()}${every(5, ss())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${mm()}${every(6, ss())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${mm()}${every(10, ss())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${mm()}${every(12, ss())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${mm()}${every(15, ss())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${mm()}${every(20, ss())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${mm()}${every(30, ss())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${every(1, mm())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${every(2, mm())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${every(3, mm())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${every(4, mm())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${every(5, mm())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${every(6, mm())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${every(10, mm())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${every(12, mm())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${every(15, mm())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${every(20, mm())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${hh()}${every(30, mm())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${every(1, hh())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${every(2, hh())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${every(3, hh())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${every(4, hh())}"));
    Assert.assertTrue(validate("/${YY()}${MM()}${DD()}${every(6, hh())}"));
  }

  @Test
  public void validateInvalid() {
    Assert.assertFalse(validate("/${foo}"));
    Assert.assertFalse(validate("/${YY()}/${DD()}"));
    Assert.assertFalse(validate("/${YY()}${MM()}${DD()}${hh()}${mm()}${every(2, 'x')}"));
    Assert.assertFalse(validate("/${YY()}${MM()}${DD()}${hh()}${mm()}${every(2, mm())}${every(3, mm())}"));
    Assert.assertFalse(validate("/${YY()}${MM()}${DD()}${hh()}${mm()}${every(0, ss())}"));
    Assert.assertFalse(validate("/${YY()}${MM()}${DD()}${hh()}${mm()}${every(60, ss())}"));
    Assert.assertFalse(validate("/${YY()}${MM()}${DD()}${hh()}${mm()}${every(7, mm())}"));
    Assert.assertFalse(validate("/${YY()}${MM()}${DD()}${hh()}${mm()}${every(45, mm())}"));
    Assert.assertFalse(validate("/${YY()}${MM()}${DD()}${hh()}${every(1, hh())}"));
    Assert.assertFalse(validate("/${YY()}${MM()}${DD()}${every(1, DD())}"));
    Assert.assertFalse(validate("/${YY()}${MM()}${every(1, MM())}"));
    Assert.assertFalse(validate("/${YY()}${every(1, YYYY())}"));
    Assert.assertFalse(validate("/${YY()}${MM()}${DD()}${hh()}${ss()}${every(1, mm())}"));
    Assert.assertFalse(validate("/${YY()}${MM()}${DD()}${hh()}${ss()}${every(1, hh())}"));
    Assert.assertFalse(validate("/${YY()}${MM()}${DD()}${hh()}${mm()}${every(1, mm())}"));
  }

  private Date parseDate(String str) throws Exception {
    DateFormat parser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    parser.setTimeZone(TimeZone.getTimeZone("UTC"));
    return parser.parse(str);
  }

  @Test
  public void testFloorDate() throws Exception {
    // up to seconds
    String template = "/${YYYY()}/${MM()}/${DD()}/${hh()}/${mm()}/${ss()}";
    Date actual = parseDate("2015-01-20T14:01:15Z");
    Date expected = parseDate("2015-01-20T14:01:15Z");
    Date computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());
    actual = new Date(actual.getTime() + 10);
    computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());

    // up to minutes
    template = "/${YYYY()}/${MM()}/${DD()}/${hh()}/${mm()}";
    actual = parseDate("2015-01-20T14:01:15Z");
    expected = parseDate("2015-01-20T14:01:00Z");
    computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());
    actual = new Date(actual.getTime() + 10);
    computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());

    // up to hours
    template = "/${YYYY()}/${MM()}/${DD()}/${hh()}";
    actual = parseDate("2015-01-20T14:01:15Z");
    expected = parseDate("2015-01-20T14:00:00Z");
    computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());
    actual = new Date(actual.getTime() + 10);
    computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());

    // up to days
    template = "/${YYYY()}/${MM()}/${DD()}";
    actual = parseDate("2015-01-20T14:01:15Z");
    expected = parseDate("2015-01-20T00:00:00Z");
    computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());
    actual = new Date(actual.getTime() + 10);
    computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());

    // up to months
    template = "/${YYYY()}/${MM()}";
    actual = parseDate("2015-01-20T14:01:15Z");
    expected = parseDate("2015-01-01T00:00:00Z");
    computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());
    actual = new Date(actual.getTime() + 10);
    computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());

    // up to years
    template = "/${YYYY()}";
    actual = parseDate("2015-01-20T14:01:15Z");
    expected = parseDate("2015-01-01T00:00:00Z");
    computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());
    actual = new Date(actual.getTime() + 10);
    computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());

    // leap year
    template = "/${YY()}/${MM()}";
    actual = parseDate("2016-02-29T1:01:15Z");
    expected = parseDate("2016-02-01T00:00:00Z");
    computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());
    actual = new Date(actual.getTime() + 10);
    computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());

    // up to minutes using every 1min
    template = "/${YYYY()}/${MM()}/${DD()}/${hh()}/${every(1, mm())}";
    actual = parseDate("2015-01-20T14:01:15Z");
    expected = parseDate("2015-01-20T14:01:00Z");
    computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());
    actual = new Date(actual.getTime() + 10);
    computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());

    // up to minutes using every 2min
    template = "/${YYYY()}/${MM()}/${DD()}/${hh()}/${every(2, mm())}";
    actual = parseDate("2015-01-20T14:00:15Z");
    expected = parseDate("2015-01-20T14:00:00Z");
    computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());
    actual = new Date(actual.getTime() + 10);
    computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());

    // up to minutes using every 5min first freq range
    template = "/${YYYY()}/${MM()}/${DD()}/${hh()}/${every(5, mm())}";
    actual = parseDate("2015-01-20T14:07:15Z");
    expected = parseDate("2015-01-20T14:05:00Z");
    computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());
    actual = new Date(actual.getTime() + 10);
    computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());

    // up to minutes using every 15min last freq range
    template = "/${YYYY()}/${MM()}/${DD()}/${hh()}/${every(15, mm())}";
    actual = parseDate("2015-01-20T14:46:35Z");
    expected = parseDate("2015-01-20T14:45:00Z");
    computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());
    actual = new Date(actual.getTime() + 10);
    computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertEquals(expected.getTime(), computed.getTime());

    // no date at all
    template = "/foo";
    actual = parseDate("2015-01-20T14:01:15Z");
    computed = getPathTemplateEL(template).getFloorDate(actual);
    Assert.assertNull(computed);
  }

  @Test
  public void testResolvePath() throws Exception {
    String template = "/${YYYY()}/${MM()}/${DD()}/${hh()}/${mm()}/${ss()}";
    Date date = parseDate("2015-01-20T14:01:15Z");
    String expected = "/2015/01/20/14/01/15";
    String got = getPathTemplateEL(template).resolvePath(date, RecordCreator.create());
    Assert.assertEquals(expected, got);

    template = "/${YYYY()}/${MM()}/${DD()}/${hh()}/${every(15, mm())}";
    date = parseDate("2015-01-20T14:00:00Z");
    expected = "/2015/01/20/14/00";
    got = getPathTemplateEL(template).resolvePath(date, RecordCreator.create());
    Assert.assertEquals(expected, got);

    template = "/${YYYY()}/${MM()}/${DD()}/${hh()}/${every(15, mm())}";
    date = parseDate("2015-01-20T14:01:14Z");
    expected = "/2015/01/20/14/00";
    got = getPathTemplateEL(template).resolvePath(date, RecordCreator.create());
    Assert.assertEquals(expected, got);

    template = "/${YYYY()}/${MM()}/${DD()}/${hh()}/${every(15, mm())}";
    date = parseDate("2015-01-20T14:15:00Z");
    expected = "/2015/01/20/14/15";
    got = getPathTemplateEL(template).resolvePath(date, RecordCreator.create());
    Assert.assertEquals(expected, got);

    template = "/${YYYY()}/${MM()}/${DD()}/${hh()}/${every(15, mm())}";
    date = parseDate("2015-01-20T14:31:00Z");
    expected = "/2015/01/20/14/30";
    got = getPathTemplateEL(template).resolvePath(date, RecordCreator.create());
    Assert.assertEquals(expected, got);

    template = "/${YYYY()}/${MM()}/${DD()}/${hh()}/${every(15, mm())}";
    date = parseDate("2015-01-20T14:59:59Z");
    expected = "/2015/01/20/14/45";
    got = getPathTemplateEL(template).resolvePath(date, RecordCreator.create());
    Assert.assertEquals(expected, got);


    // no date at all
    template = "/foo";
    date = parseDate("2015-01-20T14:01:15Z");
    expected = "/foo";
    got = getPathTemplateEL(template).resolvePath(date, RecordCreator.create());
    Assert.assertEquals(expected, got);

  }
}
