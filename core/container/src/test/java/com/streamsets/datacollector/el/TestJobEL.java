/*
 * Copyright 2018 StreamSets Inc.
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

import org.junit.Assert;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TestJobEL {

  @Test
  public void testUndefinedJobInformation() {
    JobEL.setConstantsInContext(null);
    Assert.assertEquals("UNDEFINED", JobEL.id());
    Assert.assertEquals("UNDEFINED", JobEL.name());
    Assert.assertEquals("UNDEFINED", JobEL.user());
  }

  @Test
  public void testJobEls() {
    Date startTime = new Date();
    Map<String, Object> parameters = new HashMap<>();
    parameters.put(JobEL.JOB_ID_VAR, "sampleJobId");
    parameters.put(JobEL.JOB_NAME_VAR, "sampleJobName");
    parameters.put(JobEL.JOB_USER_VAR, "user1@org1");
    parameters.put(JobEL.JOB_START_TIME_VAR, startTime.getTime());
    JobEL.setConstantsInContext(parameters);
    Assert.assertEquals("sampleJobId", JobEL.id());
    Assert.assertEquals("sampleJobName", JobEL.name());
    Assert.assertEquals("user1@org1", JobEL.user());
    Assert.assertEquals(startTime, JobEL.startTime());
  }

}
