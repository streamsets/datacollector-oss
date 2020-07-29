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
package com.streamsets.datacollector.restapi.bean;

import com.streamsets.datacollector.record.HeaderImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class TestHeaderBean {

  @Test(expected = NullPointerException.class)
  public void testHeaderBeanNull() {
    HeaderJson h = new HeaderJson(null);
  }

  @Test
  public void testHeaderBean() {
    HeaderImpl header = new HeaderImpl("s1", "id1", "/s1", "t1", "t0", null, "byte", "e1", "ep1", "es1", "stageName", "ec", "em",
      System.currentTimeMillis(), "stack trace", new HashMap<>(), "jId", "jName");

    HeaderJson headerJsonBean = new HeaderJson(header);

    Assert.assertEquals(header.getErrorCode(), headerJsonBean.getErrorCode());
    Assert.assertEquals(header.getErrorMessage(), headerJsonBean.getErrorMessage());
    Assert.assertEquals(header.getErrorDataCollectorId(), headerJsonBean.getErrorDataCollectorId());
    Assert.assertEquals(header.getErrorPipelineName(), headerJsonBean.getErrorPipelineName());
    Assert.assertEquals(header.getErrorStage(), headerJsonBean.getErrorStage());
    Assert.assertEquals(header.getErrorStageLabel(), headerJsonBean.getErrorStageLabel());
    Assert.assertEquals(header.getErrorTimestamp(), headerJsonBean.getErrorTimestamp());
    Assert.assertEquals(header.getErrorStackTrace(), headerJsonBean.getErrorStackTrace());
    Assert.assertEquals(header.getPreviousTrackingId(), headerJsonBean.getPreviousTrackingId());
    Assert.assertArrayEquals(header.getRaw(), headerJsonBean.getRaw());
    Assert.assertEquals(header.getRawMimeType(), headerJsonBean.getRawMimeType());
    Assert.assertEquals(header.getValues(), headerJsonBean.getValues());
    Assert.assertEquals(header.getSourceId(), headerJsonBean.getSourceId());
    Assert.assertEquals(header.getStageCreator(), headerJsonBean.getStageCreator());
    Assert.assertEquals(header.getStagesPath(), headerJsonBean.getStagesPath());
    Assert.assertEquals(header.getTrackingId(), headerJsonBean.getTrackingId());
    Assert.assertEquals(header.getErrorJobId(), headerJsonBean.getErrorJobId());
    Assert.assertEquals(header.getErrorJobName(), headerJsonBean.getErrorJobName());

  }

  @Test
  public void testHeaderBeanConstructorWithArgs() {
    long timestamp = System.currentTimeMillis();
    HeaderImpl header = new HeaderImpl("s1", "id1", "/s1", "t1", "t0", null, "byte", "e1", "ep1", "es1", "stageName", "ec", "em",
      timestamp, "stack trace", new HashMap<>(), "jId", "jName");

    HeaderJson headerJsonBean = new HeaderJson("s1", "id1", "/s1", "t1", "t0", null, "byte", "e1", "ep1", "es1", "stageName", "ec", "em",
      timestamp, "stack trace", "jId", "jName", new HashMap<>());

    Assert.assertEquals(header.getErrorCode(), headerJsonBean.getErrorCode());
    Assert.assertEquals(header.getErrorMessage(), headerJsonBean.getErrorMessage());
    Assert.assertEquals(header.getErrorDataCollectorId(), headerJsonBean.getErrorDataCollectorId());
    Assert.assertEquals(header.getErrorPipelineName(), headerJsonBean.getErrorPipelineName());
    Assert.assertEquals(header.getErrorStage(), headerJsonBean.getErrorStage());
    Assert.assertEquals(header.getErrorStageLabel(), headerJsonBean.getErrorStageLabel());
    Assert.assertEquals(header.getErrorTimestamp(), headerJsonBean.getErrorTimestamp());
    Assert.assertEquals(header.getErrorStackTrace(), headerJsonBean.getErrorStackTrace());
    Assert.assertEquals(header.getPreviousTrackingId(), headerJsonBean.getPreviousTrackingId());
    Assert.assertArrayEquals(header.getRaw(), headerJsonBean.getRaw());
    Assert.assertEquals(header.getRawMimeType(), headerJsonBean.getRawMimeType());
    Assert.assertEquals(header.getValues(), headerJsonBean.getValues());
    Assert.assertEquals(header.getSourceId(), headerJsonBean.getSourceId());
    Assert.assertEquals(header.getStageCreator(), headerJsonBean.getStageCreator());
    Assert.assertEquals(header.getStagesPath(), headerJsonBean.getStagesPath());
    Assert.assertEquals(header.getTrackingId(), headerJsonBean.getTrackingId());

    //underlying headerimpl
    Assert.assertEquals(header.getErrorCode(), headerJsonBean.getHeader().getErrorCode());
    Assert.assertEquals(header.getErrorMessage(), headerJsonBean.getHeader().getErrorMessage());
    Assert.assertEquals(header.getAttributeNames(), headerJsonBean.getHeader().getAttributeNames());
    Assert.assertEquals(header.getErrorDataCollectorId(), headerJsonBean.getHeader().getErrorDataCollectorId());
    Assert.assertEquals(header.getErrorPipelineName(), headerJsonBean.getHeader().getErrorPipelineName());
    Assert.assertEquals(header.getErrorStage(), headerJsonBean.getHeader().getErrorStage());
    Assert.assertEquals(header.getErrorStageLabel(), headerJsonBean.getErrorStageLabel());
    Assert.assertEquals(header.getErrorTimestamp(), headerJsonBean.getHeader().getErrorTimestamp());
    Assert.assertEquals(header.getErrorStackTrace(), headerJsonBean.getHeader().getErrorStackTrace());
    Assert.assertEquals(header.getPreviousTrackingId(), headerJsonBean.getHeader().getPreviousTrackingId());
    Assert.assertArrayEquals(header.getRaw(), headerJsonBean.getHeader().getRaw());
    Assert.assertEquals(header.getRawMimeType(), headerJsonBean.getHeader().getRawMimeType());
    Assert.assertEquals(header.getSourceRecord(), headerJsonBean.getHeader().getSourceRecord());
    Assert.assertEquals(header.getValues(), headerJsonBean.getHeader().getValues());
    Assert.assertEquals(header.getSourceId(), headerJsonBean.getHeader().getSourceId());
    Assert.assertEquals(header.getStageCreator(), headerJsonBean.getHeader().getStageCreator());
    Assert.assertEquals(header.getStagesPath(), headerJsonBean.getHeader().getStagesPath());
    Assert.assertEquals(header.getTrackingId(), headerJsonBean.getHeader().getTrackingId());
    Assert.assertEquals(header.getErrorJobId(), headerJsonBean.getErrorJobId());
    Assert.assertEquals(header.getErrorJobName(), headerJsonBean.getErrorJobName());

  }
}
